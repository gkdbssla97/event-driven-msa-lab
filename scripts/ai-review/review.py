"""PR 자동 코드리뷰.

OpenAI 호환 API(기본: Gemini 무료 등급)에 PR diff를 보내 라인 단위 지적을 JSON으로 받고,
diff에 실제로 있는 라인만 인라인 리뷰로 게시한다. 같은 지적은 fingerprint로 다시 달지 않는다.
"""
import hashlib
import json
import os
import re
import subprocess
import time
import urllib.request
from typing import Dict, List, Optional, Set, Tuple

REVIEW_EXTENSIONS = {'.java', '.kt', '.groovy', '.yaml', '.yml', '.gradle', '.properties', '.py'}
# 무료 등급의 요청당 토큰 한도를 넘지 않도록 배치당 diff 길이를 제한 (코드 기준 1 토큰 ≈ 3자)
MAX_BATCH_CHARS = 30_000
# 한 파일의 diff가 이보다 길면 잘라서 보냄 (잘린 뒤쪽 라인은 지적 대상에서 자연히 빠진다)
MAX_SINGLE_FILE_CHARS = 30_000
# PR 전체 diff가 이보다 크면 리뷰하지 않고 안내만 남김 — 호출 한도 소진과 품질 저하 방지
MAX_TOTAL_CHARS = 150_000
MAX_INLINE_COMMENTS = 20
# 배치 호출 사이 간격(초) — 무료 등급 분당 호출 한도 대응
BATCH_CALL_DELAY_SECONDS = 3

DEFAULT_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/openai/'
DEFAULT_MODEL = 'gemini-3.8-flash'

SUMMARY_MARKER = '<!-- ai-code-review -->'
FP_PATTERN = re.compile(r'<!-- ai-review-fp:([0-9a-f]{12}) -->')

SYSTEM_PROMPT = """당신은 Java/Spring Boot 기반 이벤트 드리븐 MSA에 정통한 시니어 코드 리뷰어입니다.
PR diff를 분석해 머지 전에 짚어야 할 문제를 라인 단위로 지적하세요.

diff의 각 줄 앞 숫자는 변경 후 파일의 라인 번호입니다. 지적의 line에는 반드시 그 숫자를 쓰세요.
번호가 없는 줄(삭제된 줄)에는 지적을 달 수 없습니다.

심각도:
- P1 Blocker: 보안 취약점, 데이터 손실 위험
- P2 Critical: 회귀 위험, 테스트 미커버
- P3 Major: 성능·가독성에 큰 영향
- P4 Minor: 변수명, 작은 리팩토링
- P5 Nit: 오타, 여백

규칙:
- 한국어 존댓말, 비판이 아닌 제안 형식
- 변경되지 않은 영역은 언급하지 않음
- 확신이 없는 지적은 하지 않음. 지적할 것이 없으면 comments를 빈 배열로
- title은 문제를 한 줄로 요약 (같은 문제에는 같은 title)
- summary는 PR 전체에 대한 2~3문장 총평"""

RESPONSE_FORMAT = {
    'type': 'json_schema',
    'json_schema': {
        'name': 'code_review',
        'strict': True,
        'schema': {
            'type': 'object',
            'properties': {
                'summary': {'type': 'string'},
                'comments': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'path': {'type': 'string'},
                            'line': {'type': 'integer'},
                            'severity': {'type': 'string', 'enum': ['P1', 'P2', 'P3', 'P4', 'P5']},
                            'title': {'type': 'string'},
                            'body': {'type': 'string'},
                        },
                        'required': ['path', 'line', 'severity', 'title', 'body'],
                        'additionalProperties': False,
                    },
                },
            },
            'required': ['summary', 'comments'],
            'additionalProperties': False,
        },
    },
}


def get_diff() -> str:
    base = os.environ['BASE_SHA']
    head = os.environ['HEAD_SHA']
    result = subprocess.run(
        ['git', 'diff', base, head],
        capture_output=True, text=True, check=True
    )
    return result.stdout


def split_diff_by_file(raw_diff: str) -> List[Tuple[str, str]]:
    """Split a unified diff into [(filename, file_diff_text), ...] for relevant source file types."""
    lines = raw_diff.splitlines(keepends=True)
    files: List[Tuple[str, str]] = []
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


HUNK_HEADER = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@')


def annotate_file_diff(file_diff: str) -> Tuple[str, Set[int]]:
    """diff 각 줄 앞에 변경 후 라인 번호를 붙이고, 인라인 지적이 가능한 라인 번호 집합을 돌려준다.

    GitHub 리뷰 댓글은 변경 후(RIGHT) 기준으로 hunk 안의 추가·문맥 라인에만 달 수 있다.
    """
    out: List[str] = []
    commentable: Set[int] = set()
    new_line: Optional[int] = None

    for line in file_diff.splitlines():
        match = HUNK_HEADER.match(line)
        if match:
            new_line = int(match.group(1))
            out.append(line)
        elif new_line is None or line.startswith('\\'):
            out.append(line)  # 파일 헤더, "\ No newline at end of file"
        elif line.startswith('-'):
            out.append(f'{"":>6} {line}')
        else:  # '+' 추가 라인 또는 ' ' 문맥 라인
            out.append(f'{new_line:>6} {line}')
            commentable.add(new_line)
            new_line += 1

    return '\n'.join(out) + '\n', commentable


def truncate_file_diff(name: str, file_diff: str) -> str:
    if len(file_diff) <= MAX_SINGLE_FILE_CHARS:
        return file_diff
    return file_diff[:MAX_SINGLE_FILE_CHARS] + f'\n\n... ({name} diff truncated — too large to display in full)\n'


def make_batches(file_diffs: List[str]) -> List[str]:
    """Group per-file diffs into batches that each fit under MAX_BATCH_CHARS."""
    batches: List[str] = []
    current_parts: List[str] = []
    current_len = 0

    for file_diff in file_diffs:
        if current_parts and current_len + len(file_diff) > MAX_BATCH_CHARS:
            batches.append(''.join(current_parts))
            current_parts, current_len = [], 0
        current_parts.append(file_diff)
        current_len += len(file_diff)

    if current_parts:
        batches.append(''.join(current_parts))

    return batches


def call_model(client, model: str, batch: str) -> dict:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': f'아래 PR diff를 리뷰해주세요:\n\n```diff\n{batch}\n```'},
        ],
        response_format=RESPONSE_FORMAT,
    )
    return json.loads(response.choices[0].message.content)


def fingerprint(path: str, title: str) -> str:
    """같은 파일의 같은 문제는 라인이 밀려도 같은 값이 되도록 라인 번호는 넣지 않는다."""
    normalized = ' '.join(title.lower().split())
    return hashlib.sha1(f'{path}|{normalized}'.encode()).hexdigest()[:12]


def select_comments(
    candidates: List[dict],
    commentable: Dict[str, Set[int]],
    already_posted: Set[str],
) -> Tuple[List[dict], int, int]:
    """diff에 없는 라인 지적과 이미 단 지적을 걸러낸다. (게시할 댓글, 무효 개수, 중복 개수)"""
    selected: List[dict] = []
    seen = set(already_posted)
    invalid = duplicate = 0

    for c in candidates:
        if c['line'] not in commentable.get(c['path'], set()):
            invalid += 1
            continue
        fp = fingerprint(c['path'], c['title'])
        if fp in seen:
            duplicate += 1
            continue
        seen.add(fp)
        selected.append({
            'path': c['path'],
            'line': c['line'],
            'side': 'RIGHT',
            'body': f"**[{c['severity']}] {c['title']}**\n\n{c['body']}\n\n<!-- ai-review-fp:{fp} -->",
        })

    return selected[:MAX_INLINE_COMMENTS], invalid, duplicate


def github_request(method: str, url: str, token: str, body: Optional[dict] = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/vnd.github+json')
    if data is not None:
        req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read() or b'null')


def posted_fingerprints(token: str, repo: str, pr_number: str) -> Set[str]:
    fps: Set[str] = set()
    page = 1
    while True:
        url = f'https://api.github.com/repos/{repo}/pulls/{pr_number}/comments?per_page=100&page={page}'
        comments = github_request('GET', url, token)
        if not comments:
            return fps
        for comment in comments:
            fps.update(FP_PATTERN.findall(comment.get('body', '')))
        page += 1


def post_review(token: str, repo: str, pr_number: str, head_sha: str, comments: List[dict]) -> None:
    url = f'https://api.github.com/repos/{repo}/pulls/{pr_number}/reviews'
    github_request('POST', url, token, {'commit_id': head_sha, 'event': 'COMMENT', 'comments': comments})
    print(f'Posted review with {len(comments)} inline comment(s)')


def find_existing_bot_comment(token: str, repo: str, pr_number: str) -> Optional[int]:
    url = f'https://api.github.com/repos/{repo}/issues/{pr_number}/comments?per_page=100'
    for comment in github_request('GET', url, token):
        if comment.get('body', '').startswith(SUMMARY_MARKER):
            return comment['id']
    return None


def post_or_update_summary(token: str, repo: str, pr_number: str, body: str) -> None:
    tagged_body = f'{SUMMARY_MARKER}\n{body}'
    existing_id = find_existing_bot_comment(token, repo, pr_number)
    if existing_id:
        github_request('PATCH', f'https://api.github.com/repos/{repo}/issues/comments/{existing_id}',
                       token, {'body': tagged_body})
    else:
        github_request('POST', f'https://api.github.com/repos/{repo}/issues/{pr_number}/comments',
                       token, {'body': tagged_body})


def main() -> None:
    token = os.environ['GITHUB_TOKEN']
    repo = os.environ['REPO']
    pr_number = os.environ['PR_NUMBER']
    head_sha = os.environ['HEAD_SHA']
    api_key = os.environ.get('AI_REVIEW_API_KEY')

    if not api_key:
        print('::warning::AI_REVIEW_API_KEY secret is not set — skipping review.')
        return

    files = split_diff_by_file(get_diff())
    if not files:
        print('No relevant source file changes found — skipping review.')
        return

    annotated: List[str] = []
    commentable: Dict[str, Set[int]] = {}
    for name, file_diff in files:
        text, lines = annotate_file_diff(file_diff)
        annotated.append(truncate_file_diff(name, text))
        commentable[name] = lines

    total_chars = sum(len(t) for t in annotated)
    if total_chars > MAX_TOTAL_CHARS:
        post_or_update_summary(token, repo, pr_number,
                               f'## 🤖 AI 코드리뷰\n\ndiff가 너무 커서({total_chars:,}자) 자동 리뷰를 건너뛰었습니다. '
                               f'PR을 나누면 리뷰할 수 있습니다.')
        return

    from openai import OpenAI  # 테스트가 SDK 없이 돌도록 실제 호출 시점에만 불러온다
    client = OpenAI(api_key=api_key, base_url=os.environ.get('AI_REVIEW_BASE_URL') or DEFAULT_BASE_URL)
    model = os.environ.get('AI_REVIEW_MODEL') or DEFAULT_MODEL

    batches = make_batches(annotated)
    print(f'{len(files)} file(s) changed -> {len(batches)} review batch(es) with {model}')

    summaries: List[str] = []
    candidates: List[dict] = []
    try:
        for i, batch in enumerate(batches, start=1):
            print(f'Reviewing batch {i}/{len(batches)} ({len(batch)} chars)...')
            result = call_model(client, model, batch)
            summaries.append(result['summary'])
            candidates.extend(result['comments'])
            if i < len(batches):
                time.sleep(BATCH_CALL_DELAY_SECONDS)
    except Exception as e:  # 외부 모델 장애가 PR을 실패로 막지 않도록 경고만 남긴다
        print(f'::warning::AI review failed: {e}')
        post_or_update_summary(token, repo, pr_number,
                               f'## 🤖 AI 코드리뷰\n\n리뷰 생성에 실패했습니다 (`{type(e).__name__}`). '
                               f'다음 커밋에서 다시 시도합니다.')
        return

    comments, invalid, duplicate = select_comments(
        candidates, commentable, posted_fingerprints(token, repo, pr_number))
    if comments:
        post_review(token, repo, pr_number, head_sha, comments)

    summary = '\n\n'.join(summaries)
    post_or_update_summary(token, repo, pr_number,
                           f'## 🤖 AI 코드리뷰 ({model})\n\n{summary}\n\n'
                           f'<sub>새 지적 {len(comments)}건 · 이미 단 지적 {duplicate}건 생략 · '
                           f'diff 밖 라인 지적 {invalid}건 제외</sub>')


if __name__ == '__main__':
    main()
