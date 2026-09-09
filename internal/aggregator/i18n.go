package aggregator

// messages holds every user-facing string the aggregator sends to Telegram,
// keyed by language then by message key. Kept as plain format strings
// (rather than a generic i18n library) because the message set is small and
// fixed — this is a self-hosted single-admin tool, not a product with a
// translator workflow.
var messages = map[string]map[string]string{
	"ru": {
		"status_up":       "работает",
		"status_down":     "недоступен",
		"status_degraded": "частично недоступен",
		"status_unknown":  "неизвестно",

		"region_up":     "доступен",
		"region_down":   "недоступен",
		"region_nodata": "нет данных",

		"first_down":     "недоступен с самого первого измерения",
		"first_degraded": "частично недоступен с самого первого измерения",
		"became":         "стал %s",
		"again":          "снова %s",

		"regions_confirm_fmt": "%s (%d/%d регионов подтверждают)%s",

		"agent_back_fmt": "🔌 Агент <code>%s</code> снова на связи.",
		"agent_down_fmt": "🔌⚠️ Агент <code>%s</code> не отвечает больше %d сек (последний раз на связи %s назад).\nЭто про сам мониторинг, не про твои сайты — регион просто перестал присылать данные.",
	},
	"en": {
		"status_up":       "up",
		"status_down":     "down",
		"status_degraded": "partially down",
		"status_unknown":  "unknown",

		"region_up":     "up",
		"region_down":   "down",
		"region_nodata": "no data",

		"first_down":     "down since the very first check",
		"first_degraded": "partially down since the very first check",
		"became":         "is now %s",
		"again":          "is back %s",

		"regions_confirm_fmt": "%s (%d/%d regions confirm)%s",

		"agent_back_fmt": "🔌 Agent <code>%s</code> is back online.",
		"agent_down_fmt": "🔌⚠️ Agent <code>%s</code> hasn't responded in over %d sec (last seen %s ago).\nThis is about your monitoring setup, not your sites — the region just stopped sending data.",
	},
}

func msg(lang, key string) string {
	if m, ok := messages[lang][key]; ok {
		return m
	}
	return messages["en"][key] // safe fallback if a language/key is ever missing
}
