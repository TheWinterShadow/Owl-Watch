import unittest
from unittest.mock import Mock, patch

from execution.core.main import main


class TestMain(unittest.TestCase):

    @patch("execution.core.main.JobRunner")
    @patch("execution.core.main.JobConfigManager")
    def test_main_success(self, mock_config_manager_class, mock_job_runner_class):
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager

        mock_job_args = {"JOB_NAME": "test-job"}
        mock_job_config = Mock()
        mock_config_manager.parse_job_arguments.return_value = mock_job_args
        mock_config_manager.create_job_config.return_value = mock_job_config

        mock_runner = Mock()
        mock_job_runner_class.return_value = mock_runner

        main()

        mock_config_manager_class.assert_called_once()
        mock_config_manager.parse_job_arguments.assert_called_once()
        mock_config_manager.create_job_config.assert_called_once_with(mock_job_args)
        mock_job_runner_class.assert_called_once_with(mock_job_config)
        mock_runner.run.assert_called_once()

    @patch("execution.core.main.JobRunner")
    @patch("execution.core.main.JobConfigManager")
    @patch("sys.exit")
    def test_main_failure(
        self, mock_sys_exit, mock_config_manager_class, mock_job_runner_class
    ):
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager

        mock_job_args = {"JOB_NAME": "test-job"}
        mock_job_config = Mock()
        mock_config_manager.parse_job_arguments.return_value = mock_job_args
        mock_config_manager.create_job_config.return_value = mock_job_config

        mock_runner = Mock()
        mock_runner.run.side_effect = Exception("ETL execution failed")
        mock_job_runner_class.return_value = mock_runner

        main()

        mock_config_manager_class.assert_called_once()
        mock_config_manager.parse_job_arguments.assert_called_once()
        mock_config_manager.create_job_config.assert_called_once_with(mock_job_args)
        mock_job_runner_class.assert_called_once_with(mock_job_config)
        mock_runner.run.assert_called_once()
        mock_sys_exit.assert_called_once_with(1)

    @patch("execution.core.main.JobRunner")
    @patch("execution.core.main.JobConfigManager")
    @patch("sys.exit")
    def test_main_config_manager_failure(
        self, mock_sys_exit, mock_config_manager_class, mock_job_runner_class
    ):
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager
        mock_config_manager.parse_job_arguments.side_effect = Exception(
            "Config parsing failed"
        )

        main()

        mock_config_manager_class.assert_called_once()
        mock_config_manager.parse_job_arguments.assert_called_once()
        mock_config_manager.create_job_config.assert_not_called()
        mock_job_runner_class.assert_not_called()
        mock_sys_exit.assert_called_once_with(1)

    @patch("execution.core.main.JobRunner")
    @patch("execution.core.main.JobConfigManager")
    @patch("sys.exit")
    def test_main_job_config_creation_failure(
        self, mock_sys_exit, mock_config_manager_class, mock_job_runner_class
    ):
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager

        mock_job_args = {"JOB_NAME": "test-job"}
        mock_config_manager.parse_job_arguments.return_value = mock_job_args
        mock_config_manager.create_job_config.side_effect = Exception(
            "Job config creation failed"
        )

        main()

        mock_config_manager_class.assert_called_once()
        mock_config_manager.parse_job_arguments.assert_called_once()
        mock_config_manager.create_job_config.assert_called_once_with(mock_job_args)
        mock_job_runner_class.assert_not_called()
        mock_sys_exit.assert_called_once_with(1)

    @patch("execution.core.main.JobRunner")
    @patch("execution.core.main.JobConfigManager")
    @patch("sys.exit")
    def test_main_job_runner_creation_failure(
        self, mock_sys_exit, mock_config_manager_class, mock_job_runner_class
    ):
        mock_config_manager = Mock()
        mock_config_manager_class.return_value = mock_config_manager

        mock_job_args = {"JOB_NAME": "test-job"}
        mock_job_config = Mock()
        mock_config_manager.parse_job_arguments.return_value = mock_job_args
        mock_config_manager.create_job_config.return_value = mock_job_config

        mock_job_runner_class.side_effect = Exception("Job runner creation failed")

        main()

        mock_config_manager_class.assert_called_once()
        mock_config_manager.parse_job_arguments.assert_called_once()
        mock_config_manager.create_job_config.assert_called_once_with(mock_job_args)
        mock_job_runner_class.assert_called_once_with(mock_job_config)
        mock_sys_exit.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
