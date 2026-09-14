import unittest

from review import annotate_file_diff, fingerprint, make_batches, select_comments

FILE_DIFF = """diff --git a/Order.java b/Order.java
--- a/Order.java
+++ b/Order.java
@@ -10,4 +10,5 @@ class Order {
     int a;
-    int b;
+    long b;
+    long c;
     int d;
\\ No newline at end of file
"""


class AnnotateFileDiffTest(unittest.TestCase):

    def test_numbers_added_and_context_lines_with_new_file_line(self):
        text, commentable = annotate_file_diff(FILE_DIFF)

        self.assertEqual(commentable, {10, 11, 12, 13})
        self.assertIn('    11 +    long b;', text)
        self.assertIn('       -    int b;', text)  # 삭제 줄은 번호 없음

    def test_multiple_hunks_restart_numbering(self):
        diff = "@@ -1,1 +1,1 @@\n-x\n+y\n@@ -50,1 +60,2 @@\n z\n+w\n"

        _, commentable = annotate_file_diff(diff)

        self.assertEqual(commentable, {1, 60, 61})


class SelectCommentsTest(unittest.TestCase):

    def comment(self, line, title='NPE 가능성', path='Order.java'):
        return {'path': path, 'line': line, 'severity': 'P2', 'title': title, 'body': '설명'}

    def test_drops_lines_outside_diff_and_unknown_files(self):
        selected, invalid, _ = select_comments(
            [self.comment(11), self.comment(99, title='다른 문제'), self.comment(11, path='Other.java')],
            {'Order.java': {10, 11}}, set())

        self.assertEqual([c['line'] for c in selected], [11])
        self.assertEqual(invalid, 2)

    def test_skips_already_posted_and_repeated_findings(self):
        posted = {fingerprint('Order.java', 'NPE 가능성')}

        selected, _, duplicate = select_comments(
            [self.comment(10), self.comment(11, title='락 누락'), self.comment(10, title='락  누락')],
            {'Order.java': {10, 11}}, posted)

        self.assertEqual([c['line'] for c in selected], [11])
        self.assertEqual(duplicate, 2)
        self.assertIn('<!-- ai-review-fp:', selected[0]['body'])


class FingerprintTest(unittest.TestCase):

    def test_ignores_case_and_whitespace_but_not_path(self):
        self.assertEqual(fingerprint('A.java', 'Lock  Missing'), fingerprint('A.java', 'lock missing'))
        self.assertNotEqual(fingerprint('A.java', 'lock missing'), fingerprint('B.java', 'lock missing'))


class MakeBatchesTest(unittest.TestCase):

    def test_keeps_files_whole_within_limit(self):
        import review
        original = review.MAX_BATCH_CHARS
        review.MAX_BATCH_CHARS = 10
        try:
            self.assertEqual(make_batches(['aaaa', 'bbbb', 'cccc']), ['aaaabbbb', 'cccc'])
        finally:
            review.MAX_BATCH_CHARS = original


if __name__ == '__main__':
    unittest.main()
