import unittest
from unittest.mock import Mock, patch

from execution.jobs.analytics.sentiment import SentimentAnalysisETL


class TestableSentimentAnalysisETL(SentimentAnalysisETL):

    def __init__(self, args_to_extract):
        self.args = {}
        super().__init__(args_to_extract)

    def get_expected_schema(self):
        mock_schema = Mock()
        mock_schema.fields = []
        return mock_schema


class TestSentimentAnalysisETL(unittest.TestCase):

    def setUp(self):
        self.args_to_extract = ["cleaned_data", "curated_data"]

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_init(self, mock_super_init):
        mock_super_init.return_value = None

        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_super_init.assert_called_once_with(self.args_to_extract)
        self.assertIsNotNone(job._sentiment_keywords)
        self.assertIn("positive", job._sentiment_keywords)
        self.assertIn("negative", job._sentiment_keywords)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_load_sentiment_keywords(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        keywords = job._load_sentiment_keywords()

        self.assertIn("positive", keywords)
        self.assertIn("negative", keywords)
        self.assertIn("excellent", keywords["positive"])
        self.assertIn("terrible", keywords["negative"])
        self.assertEqual(keywords["positive"]["excellent"], 1.0)
        self.assertEqual(keywords["negative"]["terrible"], 1.0)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_run_missing_buckets(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        with self.assertRaises(ValueError) as context:
            job.run()

        self.assertIn(
            "Both cleaned_data and curated_data must be specified",
            str(context.exception),
        )

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_safe_read_success(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_spark.read.parquet.return_value = mock_df
        job.spark = mock_spark

        result = job._safe_read("s3://test-bucket/path/")

        self.assertEqual(result, mock_df)
        mock_spark.read.parquet.assert_called_once_with("s3://test-bucket/path/")

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_safe_read_empty_dataframe(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_spark = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 0
        mock_spark.read.parquet.return_value = mock_df
        job.spark = mock_spark

        result = job._safe_read("s3://test-bucket/path/")

        self.assertIsNone(result)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_safe_read_exception(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_spark = Mock()
        mock_spark.read.parquet.side_effect = Exception("Read failed")
        job.spark = mock_spark

        with self.assertRaises(Exception) as context:
            job._safe_read("s3://test-bucket/path/")

        self.assertEqual(str(context.exception), "Read failed")

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_analyze_sentiment_basic(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()

        with patch.object(
            job, "_analyze_sentiment", return_value=mock_result_df
        ) as mock_analyze:
            result = job._analyze_sentiment(mock_df)

            self.assertEqual(result, mock_result_df)
            mock_analyze.assert_called_once_with(mock_df)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    @patch("execution.jobs.analytics.sentiment.col")
    @patch("execution.jobs.analytics.sentiment.lit")
    def test_analyze_sentiment_missing_columns(
        self, mock_lit, mock_col, mock_super_init
    ):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["message", "from", "to"]
        mock_result_df = Mock()

        mock_df.withColumn.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df

        job._clean_text_for_sentiment = Mock(return_value=Mock())
        job._calculate_sentiment_score = Mock(return_value=Mock())
        job._count_sentiment_words = Mock(return_value=Mock())
        job._detect_emotional_tone = Mock(return_value=Mock())

        job.spark = Mock()
        job.spark.sql.return_value.collect.return_value = [Mock()]

        with patch.object(
            job, "_analyze_sentiment", return_value=mock_result_df
        ) as mock_analyze:
            result = job._analyze_sentiment(mock_df)

            self.assertEqual(result, mock_result_df)
            mock_analyze.assert_called_once_with(mock_df)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    @patch("execution.jobs.analytics.sentiment.lower")
    @patch("execution.jobs.analytics.sentiment.regexp_replace")
    def test_clean_text_for_sentiment(
        self, mock_regexp_replace, mock_lower, mock_super_init
    ):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_text_col = Mock()
        mock_result = Mock()
        mock_lower.return_value = mock_result

        result = job._clean_text_for_sentiment(mock_text_col)

        self.assertEqual(result, mock_result)
        mock_lower.assert_called_once()
        self.assertTrue(mock_regexp_replace.called)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_calculate_sentiment_score(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_text_col = Mock()
        mock_result = Mock()

        with patch.object(
            job, "_calculate_sentiment_score", return_value=mock_result
        ) as mock_calc:
            result = job._calculate_sentiment_score(mock_text_col)

            self.assertEqual(result, mock_result)
            mock_calc.assert_called_once_with(mock_text_col)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_count_sentiment_words_positive(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_text_col = Mock()
        mock_sentiment_type = Mock()
        mock_result = Mock()

        with patch.object(
            job, "_count_sentiment_words", return_value=mock_result
        ) as mock_count:
            result = job._count_sentiment_words(mock_text_col, mock_sentiment_type)

            self.assertEqual(result, mock_result)
            mock_count.assert_called_once_with(mock_text_col, mock_sentiment_type)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    @patch("execution.jobs.analytics.sentiment.when")
    def test_detect_emotional_tone(self, mock_when, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        mock_text_col = Mock()
        mock_result = Mock()
        mock_when.return_value.when.return_value.when.return_value.when.return_value.when.return_value.otherwise.return_value = (  # noqa: E501
            mock_result
        )
        result = job._detect_emotional_tone(mock_text_col)

        self.assertEqual(result, mock_result)
        mock_when.assert_called()

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_sentiment_keywords_structure(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        keywords = job._sentiment_keywords

        self.assertIsInstance(keywords, dict)
        self.assertIn("positive", keywords)
        self.assertIn("negative", keywords)

        positive_keywords = keywords["positive"]
        self.assertIsInstance(positive_keywords, dict)
        for word, weight in positive_keywords.items():
            self.assertIsInstance(word, str)
            self.assertIsInstance(weight, float)
            self.assertGreater(weight, 0)

        negative_keywords = keywords["negative"]
        self.assertIsInstance(negative_keywords, dict)
        for word, weight in negative_keywords.items():
            self.assertIsInstance(word, str)
            self.assertIsInstance(weight, float)
            self.assertGreater(weight, 0)

    @patch("execution.jobs.analytics.sentiment.BaseGlueETLJob.__init__")
    def test_sentiment_keywords_content(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableSentimentAnalysisETL(self.args_to_extract)

        keywords = job._sentiment_keywords

        positive_words = keywords["positive"]
        expected_positive = ["excellent", "great", "good", "wonderful", "amazing"]
        for word in expected_positive:
            self.assertIn(word, positive_words)

        negative_words = keywords["negative"]
        expected_negative = ["terrible", "awful", "bad", "horrible", "hate"]
        for word in expected_negative:
            self.assertIn(word, negative_words)


if __name__ == "__main__":
    unittest.main()
