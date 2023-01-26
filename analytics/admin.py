from django.contrib import admin

from analytics.models import TableA
# Register your models here.

class TableAdmin(admin.ModelAdmin):
    list_display = ('day', 'top_term', 'rank')
    list_filter = ('day',)
    search_fields = ['day', 'top_term']

admin.site.register(TableA, TableAdmin)