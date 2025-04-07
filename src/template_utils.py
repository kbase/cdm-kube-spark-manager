import os
import re
from typing import Any, Dict

import yaml


def load_yaml_template(template_path: str) -> str:
    """
    Load a YAML template from a file.

    Args:
        template_path: Path to the YAML template file

    Returns:
        The template as a string
    """
    with open(template_path, "r") as f:
        return f.read()


def render_template(template: str, values: Dict[str, Any]) -> str:
    """
    Render a template by replacing placeholders with values.

    Args:
        template: Template string with ${PLACEHOLDER} variables
        values: Dictionary of values to replace placeholders

    Returns:
        Rendered template as a string
    """
    rendered = template

    # Replace all ${PLACEHOLDER} variables
    for key, value in values.items():
        placeholder = f"${{{key}}}"
        rendered = rendered.replace(placeholder, str(value))

    return rendered


def render_yaml_template(template_path: str, values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load, render, and parse a YAML template.

    Args:
        template_path: Path to the YAML template file
        values: Dictionary of values to replace placeholders

    Returns:
        Parsed YAML as a dictionary
    """
    template = load_yaml_template(template_path)
    rendered = render_template(template, values)
    return yaml.safe_load(rendered)
