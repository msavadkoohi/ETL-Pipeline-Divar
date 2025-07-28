from django.contrib.postgres.fields import ArrayField
from django.db import models

class YellowWordsMetrics(models.Model):
    """
    A model to store metrics related to yellow words in advertisements.
    Fields:
    - date: Date of the metric (indexed, not nullable)
    - yellow_word: The yellow word identified (nullable)
    - queue: Queue associated with the ad (nullable)
    - dest_category: Destination category of the ad (nullable)
    - reject_reason_id: ID of the rejection reason (nullable)
    - total_count: Total count of ads (not nullable)
    - used_solo: Count of ads with only yellow word (not nullable)
    - used_set: Share of ads with yellow word set (not nullable)
    - accepted: Count of accepted ads (not nullable)
    - accepted_solo: Count of accepted ads with only yellow word (not nullable)
    - accepted_set: Share of accepted ads with yellow word set (not nullable)
    - related_rejected: Count of related rejected ads (not nullable)
    - related_rejected_solo: Count of related rejected ads with only yellow word (not nullable)
    - related_rejected_set: Share of related rejected ads with yellow word set (not nullable)
    - not_related_rejected: Count of not related rejected ads (not nullable)
    - not_related_rejected_solo: Count of not related rejected ads with only yellow word (not nullable)
    - not_related_rejected_set: Share of not related rejected ads with yellow word set (not nullable)
    """
    date = models.DateField(db_index=True, null=False)
    yellow_word = models.CharField(max_length=100, null=True, blank=True)
    queue = models.CharField(max_length=100, null=True, blank=True)
    dest_category = models.CharField(max_length=100, null=True, blank=True)
    reject_reason_id = models.IntegerField(null=True)

    total_count = models.IntegerField(null=False)
    used_solo = models.IntegerField(null=False)
    used_set = models.FloatField(null=False)

    accepted = models.IntegerField(null=False)
    accepted_solo = models.IntegerField(null=False)
    accepted_set = models.FloatField(null=False)

    related_rejected = models.IntegerField(null=False)
    related_rejected_solo = models.IntegerField(null=False)
    related_rejected_set = models.FloatField(null=False)

    not_related_rejected = models.IntegerField(null=False)
    not_related_rejected_solo = models.IntegerField(null=False)
    not_related_rejected_set = models.FloatField(null=False)

    class Meta:
        unique_together = ("date", "yellow_word", "queue", "dest_category", "reject_reason_id")
        app_label = "review"