##############################################################################
# billing_alarm.tf — AWS Budget Alert (stop surprise charges)
# Set this up BEFORE deploying anything else!
##############################################################################

# ── AWS Budget: Alert if monthly spend > $6 ──────────────────────────────────
resource "aws_budgets_budget" "monthly_cost_alarm" {
  name         = "${var.project_name}-${var.environment}-monthly-budget"
  budget_type  = "COST"
  limit_amount = "6"           # $6 USD = ~₹500 per month
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80       # Alert at 80% of budget (~$4.80)
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100      # Alert when budget exceeded ($6+)
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}


