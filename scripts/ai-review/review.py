import os
import json
import subprocess
import time
import urllib.request
import urllib.error

from openai import OpenAI

REVIEW_EXTENSIONS = {'.java', '.kt', '.groovy', '.yaml', '.yml', '.gradle', '.properties'}
# gpt-4o (GitHub Models 무료 등급)는 요청당 8000 토큰 한도.
# 시스템 프롬프트(~700 토큰) + 응답(1500 토큰)을 제외하고 배치당 diff에 쓸 수 있는 여유를 보수적으로 잡음.
# 코드 diff 기준 1 토큰 ≈ 3자로 추정 → 12,000자 ≈ 4000토큰
MAX_BATCH_CHARS = 12_000
# 한 파일의 diff 자체가 이 길이를 넘으면 해당 파일만 잘라서 별도 배치로 처리
MAX_SINGLE_FILE_CHARS = 12_000
# 배치 호출 사이 간격(초) — GitHub Models 무료 등급 rate limit 대응
BATCH_CALL_DELAY_SECONDS = 3

SYSTEM_PROMPT = """당신은 Java/Spring Boot 기반 이벤트 드리븐 MSA에 정통한 시니어 코드 리뷰어입니다.
PR diff를 분석하여 아래 형식으로 한국어 코드리뷰를 작성하세요.

## ✅ 잘된 점
잘 작성된 부분을 구체적으로 언급하세요.

## 🔧 개선 제안
우선순위 기준으로 제안하세요:
- **[P1] Blocker**: 머지 전 반드시 수정 — 보안 취약점, 데이터 손실 위험
- **[P2] Critical**: 머지 전 강하게 권장 — 테스트 미커버, 회귀 위험
- **[P3] Major**: 후속 PR 수정 — 성능·가독성에 큰 영향
- **[P4] Minor**: 선택 사항 — 변수명, 주석, 작은 리팩토링
- **[P5] Nit**: 단순 의견 — 오타, 여백

## ❓ 질문
이해가 필요한 부분을 질문하세요. "왜?" 대신 "어떤 의도였나요?" 형식으로 작성하세요.

규칙:
- 존댓말로 작성하세요
- 비판이 아닌 제안 형식으로 작성하세요
- 변경이 없는 영역은 언급하지 마세요
- 개선 제안이 없으면 해당 섹션을 생략하세요"""


def get_diff() -> str:
    base = os.environ['BASE_SHA']
    head = os.environ['HEAD_SHA']
    result = subprocess.run(
        ['git', 'diff', base, head],
        capture_output=True, text=True, check=True
    )
    return result.stdout


def split_diff_by_file(raw_diff: str) -> list[tuple[str, str]]:
    """Split a unified diff into [(filename, file_diff_text), ...] for relevant source file types."""
    lines = raw_diff.splitlines(keepends=True)
    files: list[tuple[str, str]] = []
    current_name, current_lines = None, []

    def flush():
        if current_name is not None and current_lines:
            files.append((current_name, ''.join(current_lines)))

    for line in lines:
        if line.startswith('diff --git'):
            flush()
            current_name, current_lines = None, []
            if any(line.rstrip().endswith(ext) for ext in REVIEW_EXTENSIONS):
                # "diff --git a/foo.java b/foo.java" -> "foo.java"
                current_name = line.rstrip().split(' b/')[-1]
                current_lines = [line]
        elif current_name is not None:
            current_lines.append(line)
    flush()
    return files


def truncate_file_diff(name: str, file_diff: str) -> str:
    if len(file_diff) <= MAX_SINGLE_FILE_CHARS:
        return file_diff
    return file_diff[:MAX_SINGLE_FILE_CHARS] + f'\n\n... ({name} diff truncated — too large to display in full)\n'


def make_batches(files: list[tuple[str, str]]) -> list[str]:
    """Group per-file diffs into batches that each fit under MAX_BATCH_CHARS."""
    batches: list[str] = []
    current_parts: list[str] = []
    current_len = 0

    for name, file_diff in files:
        file_diff = truncate_file_diff(name, file_diff)
        if current_parts and current_len + len(file_diff) > MAX_BATCH_CHARS:
            batches.append(''.join(current_parts))
            current_parts, current_len = [], 0
        current_parts.append(file_diff)
        current_len += len(file_diff)

    if current_parts:
        batches.append(''.join(current_parts))

    return batches


def call_github_models(diff: str) -> str:
    client = OpenAI(
        base_url='https://models.inference.ai.azure.com',
        api_key=os.environ['GITHUB_TOKEN'],
    )
    response = client.chat.completions.create(
        model='gpt-4o',
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': f'아래 PR diff를 리뷰해주세요:\n\n```diff\n{diff}\n```'},
        ],
        max_tokens=1500,
    )
    return response.choices[0].message.content


def find_existing_bot_comment(token: str, repo: str, pr_number: str) -> int | None:
    url = f'https://api.github.com/repos/{repo}/issues/{pr_number}/comments'
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/vnd.github.v3+json')
    with urllib.request.urlopen(req) as resp:
        comments = json.loads(resp.read())
    for comment in comments:
        if comment.get('body', '').startswith('<!-- ai-code-review -->'):
            return comment['id']
    return None


def post_or_update_comment(token: str, repo: str, pr_number: str, body: str) -> None:
    tagged_body = f'<!-- ai-code-review -->\n{body}'
    existing_id = find_existing_bot_comment(token, repo, pr_number)

    if existing_id:
        url = f'https://api.github.com/repos/{repo}/issues/comments/{existing_id}'
        method = 'PATCH'
    else:
        url = f'https://api.github.com/repos/{repo}/issues/{pr_number}/comments'
        method = 'POST'

    data = json.dumps({'body': tagged_body}).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Accept', 'application/vnd.github.v3+json')

    with urllib.request.urlopen(req) as resp:
        action = 'Updated' if existing_id else 'Posted'
        print(f'{action} review comment (HTTP {resp.status})')


def main() -> None:
    token = os.environ['GITHUB_TOKEN']
    repo = os.environ['REPO']
    pr_number = os.environ['PR_NUMBER']

    raw_diff = get_diff()
    files = split_diff_by_file(raw_diff)

    if not files:
        print('No relevant source file changes found — skipping review.')
        return

    batches = make_batches(files)
    print(f'{len(files)} file(s) changed -> {len(batches)} review batch(es)')

    reviews = []
    for i, batch in enumerate(batches, start=1):
        print(f'Reviewing batch {i}/{len(batches)} ({len(batch)} chars)...')
        review = call_github_models(batch)
        reviews.append(review if len(batches) == 1 else f'### 📦 배치 {i}/{len(batches)}\n\n{review}')
        if i < len(batches):
            time.sleep(BATCH_CALL_DELAY_SECONDS)

    combined = '\n\n---\n\n'.join(reviews)
    post_or_update_comment(token, repo, pr_number, combined)


if __name__ == '__main__':
    main()
