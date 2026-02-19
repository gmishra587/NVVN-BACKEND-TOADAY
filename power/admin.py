from django.contrib import admin
from .models import RegionHourlyLoad, StateDailyLoad, StateLoad5Min, Weather, DailyPredictionHistory





@admin.register(RegionHourlyLoad)
class RegionHourlyLoadAdmin(admin.ModelAdmin):
    list_display = ("region", "datetime", "load_mw")
    # list_filter = ("region", "datetime")
    search_fields = ("region",)
    ordering = ("-datetime",)




@admin.register(StateDailyLoad)
class StateDailyLoadAdmin(admin.ModelAdmin):
    list_display = ("state", "date", "energy_mu")
    # list_filter = ("state", "date")
    search_fields = ("state",)
    ordering = ("-date",)




@admin.register(StateLoad5Min)
class StateLoad5MinAdmin(admin.ModelAdmin):
    list_display = ("state","datetime","load_mw","brpl","bypl","ndpl","ndmc","mes",)
    list_filter = ("state",)
    search_fields = ("state",'datetime')
    ordering = ("-datetime",)



@admin.register(DailyPredictionHistory)
class DailyPredictionHistoryAdmin(admin.ModelAdmin):
    list_display = ("state", "date", "load_mw", "created_at")
    # list_filter = ("state", "date")
    search_fields = ("state",)
    ordering = ("-date",)



@admin.register(Weather)
class WeatherAdmin(admin.ModelAdmin):
    list_display = [
        "state", "datetime", "temperature_c", "humidity_pct", "rain_mm",
        "wind_speed_ms", "frequency", "source",
    ]
    # list_filter = ["state", "frequency", "source"]
    search_fields = ["state", "datetime"]
    ordering = ["-datetime"]
