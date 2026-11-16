"""Exercise release projection with real Git merges, including unsafe conflicts."""

from pathlib import Path
import tempfile
import unittest

from prepare_fork_release_tree import git, prepare_tree


WORKFLOW = ".github/workflows/docker-publish.yml"


class PrepareForkReleaseTreeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="axonhub-release-test-")
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("config", "core.autocrlf", "false")
        self.write(WORKFLOW, "name: release\nref: release\n")
        self.write("application.txt", "release code\n")
        self.release = self.commit("upstream release")
        self.write(WORKFLOW, "name: release\nref: upstream unstable\n")
        self.write("application.txt", "newer unstable code\n")
        self.write("unstable-only.txt", "not part of the release\n")
        self.tip = self.commit("upstream unstable after release")

    def git(self, *args):
        return git(*args, cwd=self.repo).stdout.decode("utf-8").strip()

    def write(self, path, contents):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents.encode("utf-8"))

    def commit(self, message):
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", message)
        return self.git("rev-parse", "HEAD")

    def project(self, fork):
        return prepare_tree(self.tip, self.release, fork, cwd=self.repo)

    def test_preserves_fork_workflow_and_exact_release_code(self):
        self.write(WORKFLOW, "name: release\nref: fork\n")
        self.write("fork-only.txt", "fork feature\n")
        fork = self.commit("fork publishing changes")
        self.write("staged.txt", "keep staged changes\n")
        self.git("add", "staged.txt")
        self.write("application.txt", "keep working changes\n")
        before = self.git("status", "--porcelain")

        tree = self.project(fork)

        self.assertEqual(self.git("show", f"{tree}:{WORKFLOW}"), "name: release\nref: fork")
        self.assertEqual(self.git("show", f"{tree}:application.txt"), "release code")
        self.assertEqual(self.git("show", f"{tree}:fork-only.txt"), "fork feature")
        self.assertNotIn("unstable-only.txt", self.git("ls-tree", "--name-only", tree))
        self.assertEqual(self.git("status", "--porcelain"), before)
        self.assertEqual(self.git("rev-parse", "HEAD"), fork)

    def test_rejects_application_conflicts_even_with_workflow_conflict(self):
        self.write(WORKFLOW, "name: release\nref: fork\n")
        self.write("application.txt", "fork application change\n")
        fork = self.commit("fork application and workflow changes")
        before = self.git("status", "--porcelain")
        with self.assertRaisesRegex(RuntimeError, "application.txt"):
            self.project(fork)
        self.assertEqual(self.git("status", "--porcelain"), before)
        self.assertEqual(self.git("rev-parse", "HEAD"), fork)

    def test_keeps_normal_three_way_merge_when_there_is_no_conflict(self):
        self.write("fork-only.txt", "fork feature\n")
        fork = self.commit("non-conflicting fork change")
        tree = self.project(fork)
        self.assertEqual(self.git("show", f"{tree}:{WORKFLOW}"), "name: release\nref: release")
        self.assertEqual(self.git("show", f"{tree}:application.txt"), "release code")
        self.assertEqual(self.git("show", f"{tree}:fork-only.txt"), "fork feature")

    def test_preserves_intentional_deletion_of_fork_workflow(self):
        (self.repo / WORKFLOW).unlink()
        fork = self.commit("remove fork publishing workflow")
        tree = self.project(fork)
        self.assertEqual(self.git("ls-tree", tree, "--", WORKFLOW), "")

    def test_rejects_conflicts_in_other_workflows(self):
        other = ".github/workflows/test.yml"
        self.write(other, "name: original\n")
        release = self.commit("another upstream release")
        self.write(other, "name: upstream\n")
        tip = self.commit("upstream test change")
        self.write(other, "name: fork\n")
        fork = self.commit("fork test change")
        with self.assertRaisesRegex(RuntimeError, "test.yml"):
            prepare_tree(tip, release, fork, cwd=self.repo)

    def test_reports_invalid_refs_as_errors(self):
        with self.assertRaises(RuntimeError):
            prepare_tree("missing-ref", self.release, "HEAD", cwd=self.repo)


if __name__ == "__main__":
    unittest.main()
