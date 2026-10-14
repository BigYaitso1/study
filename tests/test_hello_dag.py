
import os
import unittest

from airflow.models import DagBag


DAGS_FOLDER = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "dags")
)


class TestHelloDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_bag = DagBag(
            dag_folder=DAGS_FOLDER,
            include_examples=False,
            read_dags_from_db=False,
        )
        # Берём DAG прямо из загруженных файлов,
        # не обращаясь к метабазе Airflow.
        cls.dag = cls.dag_bag.dags.get("hello_dag")

    def test_dag_is_loaded(self):
        self.assertIsNotNone(self.dag)

    def test_dag_has_no_import_errors(self):
        self.assertEqual(self.dag_bag.import_errors, {})

    def test_dag_has_expected_task(self):
        self.assertIn("hello_task", self.dag.task_ids)

    def test_dag_has_no_schedule(self):
        self.assertIsNone(self.dag.schedule)

    def test_dag_does_not_catchup(self):
        self.assertFalse(self.dag.catchup)


if __name__ == "__main__":
    unittest.main()
