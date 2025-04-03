from app import db

class ReportData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    org_id = db.Column(db.String(50), nullable=False)
    data = db.Column(db.Text, nullable=False)
