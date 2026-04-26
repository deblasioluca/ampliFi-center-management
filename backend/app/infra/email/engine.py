"""Email notification engine (§09, §11).

SMTP integration with Jinja2 templated messages. Supports SINGLE
(one recipient per message) and LIST (multiple recipients) modes.
"""

from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import structlog

logger = structlog.get_logger()

# ── Built-in templates ────────────────────────────────────────────────────

TEMPLATES: dict[str, dict[str, str]] = {
    "review_invitation": {
        "subject": "ampliFi: Review Required — {wave_name}",
        "body": """Dear {reviewer_name},

You have been assigned to review cost centers in wave "{wave_name}".

Please use the following link to access your review scope:
{review_url}

This link expires on {expires_at}.

Scope: {scope_name}
Total items to review: {item_count}

Best regards,
ampliFi Center Management
""",
    },
    "review_reminder": {
        "subject": "ampliFi: Reminder — Review Pending for {wave_name}",
        "body": """Dear {reviewer_name},

This is a reminder that you have pending review items in wave "{wave_name}".

Items reviewed: {reviewed_count} of {total_count}
Review link: {review_url}

Please complete your review by {deadline}.

Best regards,
ampliFi Center Management
""",
    },
    "housekeeping_notification": {
        "subject": "ampliFi: Housekeeping Review — {period}",
        "body": """Dear {owner_name},

The following cost centers under your ownership have been flagged for review
in the {period} housekeeping cycle:

{flagged_centers}

Please review and confirm the appropriate action (KEEP / CLOSE / DEFER):
{review_url}

Best regards,
ampliFi Center Management
""",
    },
    "wave_signed_off": {
        "subject": "ampliFi: Wave {wave_name} Signed Off",
        "body": """Dear {admin_name},

Wave "{wave_name}" has been signed off by all reviewers.

Summary:
- Total centers: {total_centers}
- Approved: {approved_count}
- Rejected: {rejected_count}
- Pending export: {pending_export}

You can now proceed with the MDG export.

Best regards,
ampliFi Center Management
""",
    },
    "password_reset": {
        "subject": "ampliFi: Password Reset",
        "body": """Dear {user_name},

A password reset was requested for your ampliFi account.

Click the link below to set a new password:
{reset_url}

This link expires in {expires_minutes} minutes.

If you did not request this reset, please ignore this email.

Best regards,
ampliFi Center Management
""",
    },
}


def _render_template(template_name: str, context: dict) -> tuple[str, str]:
    """Render an email template with context variables."""
    tmpl = TEMPLATES.get(template_name)
    if not tmpl:
        raise ValueError(f"Unknown email template: {template_name}")

    subject = tmpl["subject"]
    body = tmpl["body"]

    for key, value in context.items():
        placeholder = f"{{{key}}}"
        subject = subject.replace(placeholder, str(value))
        body = body.replace(placeholder, str(value))

    return subject, body


class EmailEngine:
    """Sends emails via SMTP with template support."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1025,
        username: str = "",
        password: str = "",
        use_tls: bool = False,
        from_address: str = "noreply@amplifi.dev",
        from_name: str = "ampliFi Center Management",
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._use_tls = use_tls
        self._from_address = from_address
        self._from_name = from_name

    def send(
        self,
        to: str | list[str],
        template_name: str,
        context: dict,
        cc: list[str] | None = None,
    ) -> bool:
        """Send a templated email."""
        recipients = [to] if isinstance(to, str) else to
        subject, body = _render_template(template_name, context)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{self._from_name} <{self._from_address}>"
        msg["To"] = ", ".join(recipients)
        if cc:
            msg["Cc"] = ", ".join(cc)

        msg.attach(MIMEText(body, "plain"))

        # Simple HTML version
        html_body = body.replace("\n", "<br>\n")
        html = f"<html><body style='font-family: sans-serif;'>{html_body}</body></html>"
        msg.attach(MIMEText(html, "html"))

        all_recipients = list(recipients)
        if cc:
            all_recipients.extend(cc)

        try:
            if self._use_tls:
                server = smtplib.SMTP(self._host, self._port)
                server.starttls()
            else:
                server = smtplib.SMTP(self._host, self._port)

            if self._username:
                server.login(self._username, self._password)

            server.sendmail(self._from_address, all_recipients, msg.as_string())
            server.quit()

            logger.info(
                "email.sent",
                template=template_name,
                to=recipients,
                subject=subject,
            )
            return True

        except Exception as e:
            logger.error("email.send_error", error=str(e), template=template_name, to=recipients)
            return False

    def send_bulk(
        self,
        recipients: list[dict],
        template_name: str,
        common_context: dict,
    ) -> dict[str, bool]:
        """Send individualized emails to multiple recipients.

        Each recipient dict must have 'email' and can have additional
        context overrides.
        """
        results: dict[str, bool] = {}
        for recipient in recipients:
            email = recipient["email"]
            ctx = {**common_context, **recipient}
            results[email] = self.send(email, template_name, ctx)
        return results

    def test_connection(self) -> dict:
        """Test SMTP connectivity."""
        try:
            server = smtplib.SMTP(self._host, self._port, timeout=10)
            if self._use_tls:
                server.starttls()
            if self._username:
                server.login(self._username, self._password)
            server.quit()
            return {"status": "ok", "host": self._host, "port": self._port}
        except Exception as e:
            return {"status": "error", "host": self._host, "port": self._port, "error": str(e)}
