import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, meta, StrictUndefined, exceptions as jinja_exceptions

# Configure Logging
logging.basicConfig(
    filename='jinja_template_eval.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def today(format="%Y-%m-%d"):
    return datetime.today().strftime(format)

def today_minus(days=0, format="%Y-%m-%d"):
    return (datetime.today() - timedelta(days=days)).strftime(format)

def today_plus(days=0, format="%Y-%m-%d"):
    return (datetime.today() + timedelta(days=days)).strftime(format)

def month_start(format="%Y-%m-%d"):
    d = datetime.today().replace(day=1)
    return d.strftime(format)

def month_end(format="%Y-%m-%d"):
    d = datetime.today().replace(day=1) + relativedelta(months=1) - timedelta(days=1)
    return d.strftime(format)


class JinjaTemplateEvaluator:
    def __init__(self):
        self.env = Environment(undefined=StrictUndefined)

        # Add custom filters
        self.env.filters.update({
            "today": today,
            "today_minus": today_minus,
            "today_plus": today_plus,
            "month_start": month_start,
            "month_end": month_end
        })

    def evaluate(self, template_str: str, context: Dict[str, Any]) -> str:
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
        parsed_content = self.env.parse(template_str)
        return meta.find_undeclared_variables(parsed_content)


# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    evaluator = JinjaTemplateEvaluator()

    examples = [
        {
            "description": "Static date using today",
            "template": "{{ today() }}",
            "context": {}
        },
        {
            "description": "Date 7 days ago",
            "template": "{{ today_minus(days=7) }}",
            "context": {}
        },
        {
            "description": "Start of this month",
            "template": "{{ month_start() }}",
            "context": {}
        },
        {
            "description": "End of this month",
            "template": "{{ month_end() }}",
            "context": {}
        },
        {
            "description": "Future date 10 days ahead",
            "template": "{{ today_plus(days=10) }}",
            "context": {}
        }
    ]

    for example in examples:
        try:
            print(f"\\n--- {example['description']} ---")
            output = evaluator.evaluate(example['template'], example['context'])
            print("Rendered Output:", output)
            print("Variables Used:", evaluator.get_template_variables(example['template']))
        except Exception as e:
            print(f"Error: {e}")
