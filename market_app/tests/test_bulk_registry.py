from market_monitor.bulk.registry import load_bulk_sources


def test_load_bulk_sources_defaults():
    config = {
        "bulk": {
            "sources": [
                {
                    "name": "stooq",
                    "base_url": "https://stooq.pl/q/d/l",
                    "symbol_template": "?s={symbol}.us&i=d",
                    "file_extension": "",
                },
                {
                    "name": "treasury",
                    "base_url": "https://example.gov",
                    "static_path": "/daily.csv",
                },
            ]
        }
    }
    sources = load_bulk_sources(config)

    assert [source.name for source in sources] == ["stooq", "treasury"]
    assert sources[0].symbol_template == "?s={symbol}.us&i=d"
    assert sources[1].static_path == "/daily.csv"
