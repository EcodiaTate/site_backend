def send_mail(to: str, subject: str, text: str):
    # integrate Postmark/SendGrid here
    print(f"[MAIL] to={to} subject={subject}\n{text}\n")
