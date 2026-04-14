# dags/medium_rag_utils/base_collector.py
from abc import ABC, abstractmethod

class BaseCollector(ABC):
    """Abstract base class define the interface for any RAG data source."""
    
    @abstractmethod
    def extract(self, *args, **kwargs):
        """Logic to fetch raw data from source."""
        pass

    @abstractmethod
    def validate(self, *args, **kwargs):
        """Logic to check data against contracts."""
        pass

    @abstractmethod
    def load(self, *args, **kwargs):
        """Logic to load data to the warehouse."""
        pass
    
    @abstractmethod
    def index(self, *args, **kwargs):
        """Logic to generate vectors/indices in the warehouse."""
        pass
