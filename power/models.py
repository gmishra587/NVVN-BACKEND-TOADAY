from django.db import models




class WeatherSource(models.TextChoices):
    OPEN_METEO = "open_meteo", "Open Meteo"
    NASA_POWER = "nasa_power", "NASA POWER"
    IMD = "imd", "IMD"
    MANUAL = "manual", "Manual"

class WeatherFrequency(models.TextChoices):
    FIVE_MIN = "5min", "5 Minutes"
    FIFTEEN_MIN = "15min", "15 Minutes"
    HOURLY = "hourly", "Hourly"
    DAILY = "daily", "Daily"






class RegionHourlyLoad(models.Model):
    region = models.CharField(max_length=5)
    datetime = models.DateTimeField()
    load_mw = models.FloatField()



class StateDailyLoad(models.Model):
    state = models.CharField(max_length=50)
    date = models.DateField()
    energy_mu = models.FloatField()





class StateLoad5Min(models.Model):
    state = models.CharField(max_length=10)
    datetime = models.DateTimeField()

    load_mw = models.FloatField(null=True, blank=True)   # Total State Load
    brpl = models.FloatField(null=True, blank=True)
    bypl = models.FloatField(null=True, blank=True)
    ndpl = models.FloatField(null=True, blank=True)
    ndmc = models.FloatField(null=True, blank=True)
    mes = models.FloatField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["state", "datetime"],
                name="unique_state_datetime_5min"
            )
        ]
        ordering = ["-datetime"]

    def __str__(self):
        return f"{self.state} @ {self.datetime}"








class Weather(models.Model):
    state = models.CharField(max_length=50)
    datetime = models.DateTimeField(db_index=True)

    frequency = models.CharField(max_length=10)

    temperature_c = models.FloatField()
    humidity_pct = models.FloatField(null=True, blank=True)
    rain_mm = models.FloatField(null=True, blank=True)
    wind_speed_ms = models.FloatField(null=True, blank=True)

    source = models.CharField(max_length=20)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["state", "datetime", "frequency", "source"],
                name="unique_state_datetime_weather"
            )
        ]






class DailyPredictionHistory(models.Model):
    state = models.CharField(max_length=10)   # DL, MH, TN
    date = models.DateField()
    load_mw = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("state", "date")
        ordering = ["-date"]