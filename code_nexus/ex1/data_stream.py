from typing import Any
from abc import ABC, abstractmethod


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
        self.name = "Numeric Processor"

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
        self.name = "Text Processor"

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
        self.name = "Log Processor"

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


class DataStream():
    def __init__(self):
        self.processors = []

    def register_processor(self, procsr: DataProcessor) -> None:
        self.processors.append(procsr)

    def process_stream(self, stream: list[Any]) -> None:
        n = 0
        while (n < len(stream)):
            element = stream[n]
            found = False
            p = 0
            while p < len(self.processors):
                if self.processors[p].validate(element):
                    self.processors[p].ingest(element)
                    found = True
                    break
                p += 1
            if not found:
                print(
                    f"DataStream error - Can't process element in stream: {element}")
            n += 1

    def print_processors(self) -> None:
        print("== DataStream statistics ==")
        if len(self.processors) == 0:
            print("No processor found, no data")
            return
        p = 0
        while p < len(self.processors):
            processor = self.processors[p]
            total = processor.rank + len(processor.data_list)
            remaining = len(processor.data_list)
            print(
                f"{processor.name}: total {total} items processed, remaining {remaining} on processor")
            p += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors()

    print("\nRegistering Numeric Processor")
    stream.register_processor(NumericProcessor())

    batch = ['Hello world', [3.14, -1, 2.71],
             [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
              {'log_level': 'INFO', 'log_message': 'User wil isconnected'}],
             42, ['Hi', 'five']]

    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors()

    print("\nRegistering other data processors")
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors()

    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    n = 0
    while n < 3:
        stream.processors[0].output()
        n += 1

    n = 0
    while n < 2:
        stream.processors[1].output()
        n += 1
    
    stream.processors[2].output()

    stream.print_processors()