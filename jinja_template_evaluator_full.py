
import logging
from typing import Dict, Any
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, meta, StrictUndefined, exceptions as jinja_exceptions

# Configure Logging
logging.basicConfig(
    filename='jinja_template_eval.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Date Filter Functions
def today(format="%Y-%m-%d"):
    return datetime.today().strftime(format)

def today_minus(days=0, format="%Y-%m-%d"):
    return (datetime.today() - relativedelta(days=days)).strftime(format)

def today_plus(days=0, format="%Y-%m-%d"):
    return (datetime.today() + relativedelta(days=days)).strftime(format)

def month_start(format="%Y-%m-%d"):
    return datetime.today().replace(day=1).strftime(format)

def month_end(format="%Y-%m-%d"):
    first_day_next_month = datetime.today().replace(day=1) + relativedelta(months=1)
    return (first_day_next_month - relativedelta(days=1)).strftime(format)

def month_plus(months=1, format="%Y-%m-%d"):
    return (datetime.today() + relativedelta(months=months)).strftime(format)

def month_minus(months=1, format="%Y-%m-%d"):
    return (datetime.today() - relativedelta(months=months)).strftime(format)

def year_start(format="%Y-%m-%d"):
    return datetime.today().replace(month=1, day=1).strftime(format)

def year_end(format="%Y-%m-%d"):
    return datetime.today().replace(month=12, day=31).strftime(format)

def year_plus(years=1, format="%Y-%m-%d"):
    return (datetime.today() + relativedelta(years=years)).strftime(format)

def year_minus(years=1, format="%Y-%m-%d"):
    return (datetime.today() - relativedelta(years=years)).strftime(format)

def quarter_start(format="%Y-%m-%d"):
    today = datetime.today()
    quarter = (today.month - 1) // 3 + 1
    first_month = 3 * (quarter - 1) + 1
    return today.replace(month=first_month, day=1).strftime(format)

def quarter_end(format="%Y-%m-%d"):
    today = datetime.today()
    quarter = (today.month - 1) // 3 + 1
    last_month = 3 * quarter
    first_day_next_month = today.replace(month=last_month, day=1) + relativedelta(months=1)
    return (first_day_next_month - relativedelta(days=1)).strftime(format)

def quarter_plus(quarters=1, format="%Y-%m-%d"):
    return (datetime.today() + relativedelta(months=3 * quarters)).strftime(format)

def quarter_minus(quarters=1, format="%Y-%m-%d"):
    return (datetime.today() - relativedelta(months=3 * quarters)).strftime(format)

# Jinja Template Evaluator
class JinjaTemplateEvaluator:
    def __init__(self):
        self.env = Environment(undefined=StrictUndefined)

        # Register all filters
        self.env.filters.update({
            "today": today,
            "today_minus": today_minus,
            "today_plus": today_plus,
            "month_start": month_start,
            "month_end": month_end,
            "month_plus": month_plus,
            "month_minus": month_minus,
            "year_start": year_start,
            "year_end": year_end,
            "year_plus": year_plus,
            "year_minus": year_minus,
            "quarter_start": quarter_start,
            "quarter_end": quarter_end,
            "quarter_plus": quarter_plus,
            "quarter_minus": quarter_minus
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
