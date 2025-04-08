import os
import re
from typing import Any, Dict

import yaml
from jinja2 import Template


def _load_yaml_template(template_path: str) -> str:
    """
    Load a YAML template from a file.

    Args:
        template_path: Path to the YAML template file

    Returns:
        The template as a string
    """
    with open(template_path, "r") as f:
        return f.read()


def _render_template(template: str, values: Dict[str, Any]) -> str:
    """
    Render a template using Jinja2.

    Args:
        template: Template string with Jinja2 variables
        values: Dictionary of values for template rendering

    Returns:
        Rendered template as a string
    """
    jinja_template = Template(template)
    return jinja_template.render(**values)


def render_yaml_template(template_path: str, values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load, render, and parse a YAML template.

    Args:
        template_path: Path to the YAML template file
        values: Dictionary of values for template rendering

    Returns:
        Parsed YAML as a dictionary
    """
    template = _load_yaml_template(template_path)
    rendered = _render_template(template, values)
    return yaml.safe_load(rendered)
