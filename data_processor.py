from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self) -> None:
        self.rank = 0
        self.data_list = []
        
        @abstractmethod
        def validate(self, data: Any) -> bool:
            pass
        
        @abstractmethod
        def ingest(self, data: Any) -> None:
            pass
        
        def output(self) -> tuple[int, str]:
            pass
        


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def  validate(self, data: Any) -> bool:
        if isinstance(data, (int , float)):
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
            while(n < len(data)):
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
            while(n < len(data)):
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
            while(n < len(data)):
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
                while(n < len(data)):
                    if not isinstance(data[n], dict):
                        return False
                    n += 1
                return True
            else:
                return False
    
    def ingest(self, data: dict | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper string data")
    
        if isinstance(data, list): 
            n = 0
            while(n < len(data)):
                self.data_list.append(data[n]["log_level"] + ": " + data[n]["log_message"])
                n += 1
        else:
            self.data_list.append(data["log_level"] + ": " + data["log_message"])
