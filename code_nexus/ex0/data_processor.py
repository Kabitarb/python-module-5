from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank = 0
        self.data_list: list[Any] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        item = self.data_list[0]
        self.data_list.pop(0)
        rank = self.rank
        self.rank += 1
        return (rank, item)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            n = 0
            while (n < len(data)):
                if not isinstance(data[n], (int, float)):
                    return False
                n += 1
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            n = 0
            while (n < len(data)):
                self.data_list.append(str(data[n]))
                n += 1
        else:
            self.data_list.append(str(data))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif (isinstance(data, list)):
            n = 0
            while (n < len(data)):
                if not isinstance(data[n], str):
                    return False
                n += 1
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper string data")

        if isinstance(data, list):
            n = 0
            while (n < len(data)):
                self.data_list.append(str(data[n]))
                n += 1
        else:
            self.data_list.append(str(data))


class LogProcessor(DataProcessor):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        elif (isinstance(data, list)):
            n = 0
            while (n < len(data)):
                if not isinstance(data[n], dict):
                    return False
                n += 1
            return True
        else:
            return False

    def ingest(self, data: dict | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper dictionary data")

        if isinstance(data, list):
            n = 0
            while (n < len(data)):
                self.data_list.append(
                    data[n]["log_level"] + ": " + data[n]["log_message"])
                n += 1
        else:
            self.data_list.append(
                data["log_level"] + ": " + data["log_message"])


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("\nTesting Numeric Processor...")

    numeric = NumericProcessor()

    print(f"Trying to validate input '42': {numeric.validate(42)}")
    print(f"Trying to validate input 'Hello': {numeric.validate("Hello")}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except ValueError as e:
        print("Got exception:", e)

    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    numeric.ingest(data)

    print("Extracting 3 values...")
    n = 0
    while n < 3:
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")
        n += 1

    print("\nTesting Text Processor...")

    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    data = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {data}")
    text.ingest(data)
    print("Extracting 1 values...")
    n = 0
    while (n < 1):
        rank, value = text.output()
        print(f"Text value {rank}: {value}")
        n += 1

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'hello': {log.validate('Hello')}")
    data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"Processing data: {data}")
    log.ingest(data)
    print("Extracting 2 values...")
    n = 0
    while (n < 2):
        rank, value = log.output()
        print(f"Log entry {rank}: {value}")
        n += 1
