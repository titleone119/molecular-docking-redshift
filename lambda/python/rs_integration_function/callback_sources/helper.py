import json
from abc import abstractmethod, ABC
from typing import List, Union

from logger import logger
from redshift_data.finished_event import FinishedEvent
from statement_class import StatementName


class CallbackInterface(dict, ABC):
    @classmethod
    @abstractmethod
    def get_id_name(cls) -> Union[str, None]:
        """
        This method returns the name of the callback field that has a unique ID that is associated with the callback
        source. This callback source can be an entity that submits multiple SQL statements over time.
        """
        raise NotImplemented("get_id_name must be implemented by an implementation class")

    @classmethod
    @abstractmethod
    def get_callback_fieldnames(cls) -> List[str]:
        """
        The fields that hold information for this callback type.
        """
        raise NotImplemented("get_callback_fieldnames must be implemented by an implementation class")

    def to_json(self) -> str:
        return json.dumps(self)

    def get_id(self) -> Union[str, None]:
        """
        This method returns a unique ID that is associated with the callback source. This callback source can be an
        entity that submits multiple SQL statements over time.
        """
        if self.get_id_name() is None:
            return None
        return self[self.get_id_name()]

    def __init__(self, input_dict: dict):
        """
        We only want to track fields necessary for the callback
        """
        super(CallbackInterface, self).__init__()
        for field_name in self.get_callback_fieldnames():
            self[field_name] = input_dict[field_name]

    @abstractmethod
    def send_success(self, statement_name: StatementName, event_details: FinishedEvent):
        """
        This is called for the success path. Use the callback provided interface to report success.
        """

    @abstractmethod
    def send_failure(self, statement_name: StatementName, event_details: FinishedEvent):
        """
        This is called for the failure path. Use the callback provided interface to report failure.
        """


class NoCallback(CallbackInterface):
    @classmethod
    def get_id_name(cls) -> Union[str, None]:
        return None

    def send_success(self, statement_name: StatementName, event_details: FinishedEvent):
        logger.info(f"No callback for statement {statement_name} which succeeded with {event_details}")

    def send_failure(self, statement_name: StatementName, event_details: FinishedEvent):
        logger.info(f"No callback for statement {statement_name} which failed with {event_details}")

    @classmethod
    def get_callback_fieldnames(cls) -> List[str]:
        return []

