from unittest.mock import Mock


class MockDataFrame:

    def __init__(self, columns=None, data=None):
        self.columns = columns or []
        self.data = data or []

    def count(self):
        return len(self.data)

    def filter(self, condition):
        mock_filtered = MockDataFrame(self.columns, [])
        mock_filtered.count = Mock(return_value=0)
        return mock_filtered

    def select(self, *args):
        return MockDataFrame(self.columns, self.data)

    def withColumn(self, name, column):
        new_columns = (
            self.columns + [name] if name not in self.columns else self.columns
        )
        return MockDataFrame(new_columns, self.data)

    def dropDuplicates(self, subset=None):
        return MockDataFrame(self.columns, self.data)

    def write(self):
        mock_writer = Mock()
        mock_writer.mode.return_value.parquet.return_value = None
        return mock_writer

    def __getitem__(self, key):
        mock_column = Mock()
        mock_column.isNull.return_value = Mock()
        return mock_column


def create_concrete_class(abstract_class):

    class ConcreteTestClass(abstract_class):

        def run(self):
            return Mock()

        def get_expected_schema(self):
            return Mock()

        def validate_schema(self, df):
            return True

    ConcreteTestClass.__name__ = f"Concrete{abstract_class.__name__}"
    ConcreteTestClass.__qualname__ = f"Concrete{abstract_class.__name__}"

    return ConcreteTestClass
