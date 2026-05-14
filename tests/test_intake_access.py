from types import SimpleNamespace
from typing import Optional
import unittest

from app.main import _user_can_view_intake


def _intake(assigned_engineers: Optional[str] = None) -> SimpleNamespace:
    return SimpleNamespace(assigned_engineers=assigned_engineers)


class IntakeAccessTest(unittest.TestCase):
    def test_admin_can_view_any_intake(self) -> None:
        user = {"role": "admin", "initials": "ZZ"}

        self.assertTrue(_user_can_view_intake(user, _intake('["JW"]')))

    def test_engineer_can_view_assigned_intake(self) -> None:
        user = {"role": "engineer", "initials": "JW"}

        self.assertTrue(_user_can_view_intake(user, _intake('["JW", "SW"]')))

    def test_drafter_can_view_assigned_intake_case_insensitively(self) -> None:
        user = {"role": "drafter", "initials": "sw"}

        self.assertTrue(_user_can_view_intake(user, _intake('["JW", "SW"]')))

    def test_employee_cannot_view_unassigned_intake(self) -> None:
        user = {"role": "employee", "initials": "JP"}

        self.assertFalse(_user_can_view_intake(user, _intake('["JW", "SW"]')))

    def test_employee_cannot_view_intake_without_valid_assignments(self) -> None:
        user = {"role": "employee", "initials": "JP"}

        self.assertFalse(_user_can_view_intake(user, _intake(None)))
        self.assertFalse(_user_can_view_intake(user, _intake("not-json")))


if __name__ == "__main__":
    unittest.main()
