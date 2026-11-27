import unittest

from ci.update_homebrew_formula import update_formula


class HomebrewFormulaTest(unittest.TestCase):
    targets = [
        "aarch64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
    ]
    tag = "v0.42.10"

    def setUp(self):
        self.formula = '  version "0.42.10-beta.26"\n'
        self.manifest = ""
        for index, target in enumerate(self.targets, start=1):
            self.formula += f'    url "#{{archive}}-{target}.tar.gz"\n'
            self.formula += f'    sha256 "{"0" * 64}"\n'
            entry = f"{str(index) * 64}  lancedb-{self.tag}-{target}.tar.gz\n"
            self.manifest = entry + self.manifest
        self.formula += '  desc "Command-line client for LanceDB"\n'

    def test_updates_each_platform_and_preserves_rest_of_formula(self):
        result = update_formula(self.formula, self.tag, self.manifest)
        expected = self.formula.replace("0.42.10-beta.26", "0.42.10")
        for index in range(1, 4):
            expected = expected.replace("0" * 64, str(index) * 64, 1)
        self.assertEqual(result, expected)
        self.assertEqual(update_formula(result, self.tag, self.manifest), result)

    def test_retry_of_older_release_cannot_downgrade_the_tap(self):
        formula = self.formula.replace("0.42.10-beta.26", "0.43.0")
        self.assertEqual(update_formula(formula, self.tag, self.manifest), formula)

    def test_rejects_prereleases_and_incomplete_or_ambiguous_inputs(self):
        with self.assertRaisesRegex(ValueError, "stable CLI release tag"):
            update_formula(self.formula, "v0.42.10-beta.26", self.manifest)
        for manifest in (
            self.manifest.split("\n", 1)[1],
            self.manifest + self.manifest,
            self.manifest.replace("v0.42.10", "v0.42.9"),
            self.manifest.replace("1" * 64, "invalid"),
        ):
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                update_formula(self.formula, self.tag, manifest)
        for formula in (
            self.formula.replace("aarch64-apple-darwin", "unexpected-target"),
            self.formula + self.formula,
        ):
            with self.subTest(formula=formula), self.assertRaises(ValueError):
                update_formula(formula, self.tag, self.manifest)
