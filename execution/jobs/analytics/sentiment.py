"""
Sentiment Analysis ETL Job

This module provides ETL functionality for analyzing sentiment in communication data.
It processes text data to determine sentiment scores, emotional tones, and related metrics.
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    col,
    lit,
    lower,
    regexp_replace,
    size,
    split,
    when,
)

from execution.models.base_job import BaseGlueETLJob


class SentimentAnalysisETL(BaseGlueETLJob):
    """
    ETL job for sentiment analysis of communication data.

    This job processes various communication data sources (email, Slack, WhatsApp)
    to analyze sentiment using keyword-based scoring. It produces enriched data
    with sentiment scores, labels, confidence levels, and emotional tones.

    Attributes:
        _sentiment_keywords: Dictionary containing positive and negative keywords with weights
    """

    def __init__(self, args_to_extract: List[str]):
        """Initialize sentiment analysis job with keyword dictionaries."""
        super().__init__(args_to_extract)
        self._sentiment_keywords = self._load_sentiment_keywords()

    def run(self) -> DataFrame:
        """
        Execute sentiment analysis on communication data.

        Returns:
            DataFrame with sentiment analysis results
        """
        cleaned_bucket = ""
        curated_bucket = ""

        if not cleaned_bucket or not curated_bucket:
            raise ValueError(
                "Both cleaned_data and curated_data must be specified")

        print(
            f"Processing communication data from s3://{cleaned_bucket}/communications/"
        )

        try:
            email_df = self._safe_read(
                f"s3://{cleaned_bucket}/communications/emails/")
            slack_df = self._safe_read(
                f"s3://{cleaned_bucket}/communications/slack/")
            whatsapp_df = self._safe_read(
                f"s3://{cleaned_bucket}/communications/whatsapp/"
            )

            communication_dfs = [
                df for df in [email_df, slack_df, whatsapp_df] if df is not None
            ]

            if not communication_dfs:
                try:
                    df = self.spark.read.parquet(
                        f"s3://{cleaned_bucket}/cleaned/")
                except Exception:
                    df = self.spark.read.parquet(f"s3://{cleaned_bucket}/")
            else:
                df = communication_dfs[0]
                for comm_df in communication_dfs[1:]:
                    df = df.union(comm_df)

        except Exception as e:
            print(f"Could not read communication data: {e}")
            try:
                df = self.spark.read.parquet(f"s3://{cleaned_bucket}/")
            except Exception as fallback_error:
                print(
                    f"Could not read any data from {cleaned_bucket}: {fallback_error}"
                )
                raise ValueError(
                    f"No data available in bucket {cleaned_bucket}")

        print(f"Read {df.count()} communication records for sentiment analysis")

        sentiment_df = self._analyze_sentiment(df)

        output_path = f"s3://{curated_bucket}/sentiment_analysis/"
        print(f"Writing sentiment analysis results to {output_path}")

        sentiment_df.write.mode("overwrite").parquet(output_path)

        print(
            f"Successfully analyzed sentiment for {sentiment_df.count()} records")
        return sentiment_df

    def _safe_read(self, path: str) -> Optional[DataFrame]:
        df = self.spark.read.parquet(path)
        if df.count() > 0:
            return df
        return None

    def _analyze_sentiment(self, df: DataFrame) -> DataFrame:
        required_columns = ["body", "sender", "recipient"]
        available_columns = df.columns
        for col_name in required_columns:
            if col_name not in available_columns:
                df = df.withColumn(col_name, lit(None))

        if "body" not in available_columns:
            if "message" in available_columns:
                df = df.withColumn("body", col("message"))
            elif "text" in available_columns:
                df = df.withColumn("body", col("text"))
            elif "content" in available_columns:
                df = df.withColumn("body", col("content"))

        df_clean = df.withColumn(
            "clean_text", self._clean_text_for_sentiment(col("body"))
        )

        sentiment_df = df_clean.withColumn(
            "sentiment_score", self._calculate_sentiment_score(
                col("clean_text"))
        ).withColumn(
            "sentiment_label",
            when(col("sentiment_score") > 0.1, "positive")
            .when(col("sentiment_score") < -0.1, "negative")
            .otherwise("neutral"),
        )

        sentiment_df = (
            sentiment_df.withColumn(
                "confidence", spark_abs(col("sentiment_score")))
            .withColumn("word_count", size(split(col("clean_text"), r"\s+")))
            .withColumn(
                "positive_word_count",
                self._count_sentiment_words(
                    col("clean_text"), lit("positive")),
            )
            .withColumn(
                "negative_word_count",
                self._count_sentiment_words(
                    col("clean_text"), lit("negative")),
            )
        )

        sentiment_df = sentiment_df.withColumn(
            "sentiment_intensity",
            when(spark_abs(col("sentiment_score")) >= 0.5, "strong")
            .when(spark_abs(col("sentiment_score")) >= 0.2, "moderate")
            .otherwise("weak"),
        ).withColumn("emotional_tone", self._detect_emotional_tone(col("clean_text")))

        sentiment_df = sentiment_df.withColumn(
            "analyzed_at",
            lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]),
        ).withColumn("analysis_version", lit("1.0"))

        return sentiment_df

    def _clean_text_for_sentiment(self, text_col):
        return lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(text_col, r"[^\w\s]", " "),
                    r"\s+",
                    " ",
                ),
                r"\b\w{1,2}\b",
                "",
            )
        )

    def _calculate_sentiment_score(self, text_col):
        score = lit(0.0)

        for word, weight in self._sentiment_keywords["positive"].items():
            score = score + when(
                text_col.rlike(f"\\b{word}\\b"), lit(weight)
            ).otherwise(0.0)

        for word, weight in self._sentiment_keywords["negative"].items():
            score = score - when(
                text_col.rlike(f"\\b{word}\\b"), lit(weight)
            ).otherwise(0.0)

        return when(
            size(split(text_col, r"\s+")) > 0, score /
            size(split(text_col, r"\s+"))
        ).otherwise(0.0)

    def _count_sentiment_words(self, text_col, sentiment_type):
        if sentiment_type.toString() == "positive":
            word_dict = self._sentiment_keywords["positive"]
        else:
            word_dict = self._sentiment_keywords["negative"]

        count = lit(0)
        for word in word_dict.keys():
            count = count + \
                when(text_col.rlike(f"\\b{word}\\b"), 1).otherwise(0)

        return count

    def _detect_emotional_tone(self, text_col):
        return (
            when(text_col.rlike(r"\\b(angry|furious|outraged|mad)\\b"), "anger")
            .when(
                text_col.rlike(r"\\b(excited|thrilled|amazed|wonderful)\\b"),
                "excitement",
            )
            .when(
                text_col.rlike(
                    r"\\b(sad|disappointed|unhappy|depressed)\\b"), "sadness"
            )
            .when(
                text_col.rlike(
                    r"\\b(worried|concerned|anxious|nervous)\\b"), "concern"
            )
            .when(
                text_col.rlike(
                    r"\\b(grateful|thankful|appreciative)\\b"), "gratitude"
            )
            .otherwise("neutral")
        )

    def _load_sentiment_keywords(self) -> Dict[str, Dict[str, float]]:
        return {
            "positive": {
                "excellent": 1.0,
                "great": 0.8,
                "good": 0.6,
                "wonderful": 1.0,
                "amazing": 1.0,
                "fantastic": 1.0,
                "love": 0.8,
                "like": 0.4,
                "happy": 0.8,
                "pleased": 0.6,
                "satisfied": 0.6,
                "perfect": 1.0,
                "awesome": 0.9,
                "brilliant": 0.9,
                "outstanding": 1.0,
                "superb": 0.9,
                "impressive": 0.7,
                "successful": 0.7,
                "positive": 0.6,
                "helpful": 0.5,
                "useful": 0.5,
                "appreciate": 0.6,
                "thank": 0.5,
                "congratulations": 0.8,
            },
            "negative": {
                "terrible": 1.0,
                "awful": 1.0,
                "bad": 0.6,
                "poor": 0.5,
                "worst": 1.0,
                "horrible": 1.0,
                "hate": 0.9,
                "dislike": 0.5,
                "angry": 0.8,
                "frustrated": 0.7,
                "disappointed": 0.7,
                "upset": 0.6,
                "annoyed": 0.5,
                "concerned": 0.4,
                "worried": 0.5,
                "problem": 0.5,
                "issue": 0.4,
                "error": 0.5,
                "failure": 0.7,
                "wrong": 0.4,
                "difficult": 0.3,
                "impossible": 0.6,
                "urgent": 0.3,
                "serious": 0.4,
            },
        }
