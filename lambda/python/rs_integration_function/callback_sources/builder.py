from callback_sources.helper import NoCallback
from callback_sources.sfn_source import SfnCallback


class CallbackSourceBuilder(object):
    sources = [SfnCallback]

    @classmethod
    def get_callback_class_for_event(cls, event):
        max_score = 0
        best_class = None
        for source_class in cls.sources:
            amount_matching_fields = sum(
                [1 if field in event else 0 for field in source_class.get_callback_fieldnames()]
            )
            if amount_matching_fields != len(source_class.get_callback_fieldnames()):
                # No full match for callback
                continue
            if amount_matching_fields > max_score:
                max_score = amount_matching_fields
                best_class = source_class
        if max_score == 0:
            best_class = NoCallback
        return best_class

    @classmethod
    def get_callback_object_for_event(cls, event):
        return cls.get_callback_class_for_event(event)(event)
