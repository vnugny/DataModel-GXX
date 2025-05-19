"""
jinja_template_evaluator.py

Evaluate Jinja template expressions with support for parameters and integration
into Great Expectations rule framework.
"""

import logging
from typing import Dict, Any
from jinja2 import Environment, meta, StrictUndefined, Template, exceptions as jinja_exceptions

# Configure Logging
logging.basicConfig(
    filename='jinja_template_eval.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class JinjaTemplateEvaluator:
    def __init__(self, extra_filters: Dict[str, Any] = None):
        """
        Initializes the evaluator with a strict Jinja environment.
        """
        self.env = Environment(undefined=StrictUndefined)
        if extra_filters:
            self.env.filters.update(extra_filters)

    def evaluate(self, template_str: str, context: Dict[str, Any]) -> str:
        """
        Renders a Jinja template string using the provided context.

        :param template_str: Jinja template string
        :param context: Dictionary of values to render in the template
        :return: Rendered string
        """
        try:
            template = self.env.from_string(template_str)
            rendered = template.render(context)
            logger.info(f"Rendered Template: {rendered}")
            return rendered
        except jinja_exceptions.UndefinedError as ue:
            logger.error(f"Undefined variable in template: {ue}", exc_info=True)
            raise
        except jinja_exceptions.TemplateSyntaxError as se:
            logger.error(f"Syntax error in template: {se}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during template rendering: {e}", exc_info=True)
            raise

    def get_template_variables(self, template_str: str) -> set:
        """
        Extracts all variables used in the template string.

        :param template_str: Jinja template string
        :return: Set of variable names
        """
        parsed_content = self.env.parse(template_str)
        variables = meta.find_undeclared_variables(parsed_content)
        return variables

# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    evaluator = JinjaTemplateEvaluator()

    template_str = """
    {% if country == "US" %}
    country = 'US'
    {% else %}
    country != 'US'
    {% endif %}
    """

    context = {
        "country": "US"
    }

    try:
        output = evaluator.evaluate(template_str, context)
        print("Rendered Output:")
        print(output)

        vars_used = evaluator.get_template_variables(template_str)
        print("Template Variables:", vars_used)

    except Exception as err:
        print(f"Error: {err}")
