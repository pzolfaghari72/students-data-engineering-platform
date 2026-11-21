import unittest

from spark.warehouse_keys import pseudonymize_identifier


class PseudonymizeIdentifierTests(unittest.TestCase):
    def test_student_key_remains_stable_and_compatible(self) -> None:
        first = pseudonymize_identifier("student", 17, "test-secret")
        second = pseudonymize_identifier("student", "17", "test-secret")

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)

    def test_entity_namespace_prevents_cross_dimension_linkage(self) -> None:
        student_key = pseudonymize_identifier("student", 17, "test-secret")
        applicant_key = pseudonymize_identifier("applicant", 17, "test-secret")

        self.assertNotEqual(student_key, applicant_key)

    def test_empty_secret_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            pseudonymize_identifier("student", 17, "")


if __name__ == "__main__":
    unittest.main()
