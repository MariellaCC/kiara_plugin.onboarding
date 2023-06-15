# -*- coding: utf-8 -*-
from kiara.models.values.value import Value, ValueMap


def check_results(outputs: ValueMap):
    """You can either check the whole results instance by specifying an argument called 'outputs'."""

    assert list(outputs.field_names) == ["greeting"]


def check_greeting(greeting: Value):
    """Or you can check each output value seperately by specifying the name of the output field."""

    assert greeting.data_type_name == "string"
    assert greeting.data == "Hello beautiful world!"

    assert (
        greeting.get_property_data(
            "metadata.python_class"
        ).python_class.python_class_name
        == "str"
    )
