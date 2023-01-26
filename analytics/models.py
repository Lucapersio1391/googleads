from django.db import models

# Create your models here.

class TableA(models.Model):
    day = models.CharField(db_column='Day', max_length=100)
    top_term = models.CharField(db_column='Top_Term', max_length=100)
    rank = models.IntegerField()