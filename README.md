# neon_sage_edge

A read-only Python client for the sage-kafka mirror: list topics, read messages, deserialize schema registry data (Avro or JSON Schema), and inspect consumer groups. It never joins a consumer group or commits offsets, so reading doesn't affect MirrorMaker or any other application.

Requires Python 3.9 or later, network access to the mirror's external listener (port 9094 by default) and an account on it, and optionally access to the schema registry.

## Install

From a clone of this repository:

```bash
pip install -e ".[registry]"
```

Or straight from your Git server, without cloning:

```bash
pip install "neon-sage-edge[registry] @ git+https://github.com:sagecontinuum/neon_sage_edge.git"
```

The `[registry]` extra adds the Avro and JSON Schema deserializers. `-e` installs in editable mode, so changes to the source take effect without reinstalling.

## Quick start

```python
from getpass import getpass
from neon_sage_edge import (KafkaSettings, MessageDecoder, MirrorReader, RegistrySettings,
                            expand_json, make_registry_client, show_notes, time_range)

show_notes()                                      # print the client's notes as plain lines
kafka = KafkaSettings.from_env(prompt=getpass)    # MIRROR_* environment variables
registry = RegistrySettings.from_env()            # SCHEMA_REGISTRY_* and VALUE_FORMAT / KEY_FORMAT
decoder = MessageDecoder.from_settings(registry, make_registry_client(registry))

with MirrorReader.connect(kafka, decoder) as reader:
    print(reader.topics())
    latest = reader.peek("reading.sensor.csat3", n=20)
    recent = reader.read_since("reading.sensor.csat3", minutes=15, max_messages=50_000)

print(expand_json(latest).head())
print(time_range(recent, "readout_time"))
```

`MirrorReader` also has `watermarks(topic)`, `read_all(topic)`, `read_range(topic, start, end)`, `watch(topic, seconds, on_message)` and `consumer_groups()`. Settings can be built directly instead of from the environment, for example `KafkaSettings(bootstrap_servers="host:9094", password="...")`.

## Configuration

| Environment variable | Meaning | Default |
|---|---|---|
| `MIRROR_BOOTSTRAP` | The mirror's address | `localhost:9094` |
| `MIRROR_SECURITY_PROTOCOL` | `SASL_PLAINTEXT`, or `PLAINTEXT` for a cluster without logins | `SASL_PLAINTEXT` |
| `MIRROR_USERNAME` / `MIRROR_PASSWORD` | SCRAM login | `notebook` / asked for |
| `SCHEMA_REGISTRY_URL` | Registry URL; empty means no registry | empty |
| `SCHEMA_REGISTRY_IP` | IP address to use for the registry's name, like `curl --resolve`, when your machine can't resolve it | empty |
| `VALUE_FORMAT` / `KEY_FORMAT` | `avro`, `json` or `string` | `avro` with a registry, otherwise `string` / `string` |

Records that can't be deserialized never stop a read: they're shown as text or raw bytes, and the reason is noted once per topic.

## Notebook

`notebooks/mirror-explorer.ipynb` walks through the client: topics on the mirror, the latest messages, what arrived recently (with the `readout_time` range), a live view of new messages, consumer groups, and the registry's subjects. Start Jupyter from this repository (in a virtual environment) and open it; its first code cell installs the client from the repository root.

## Layout

| Path | What's there |
|---|---|
| `src/neon_sage_edge/config.py` | `KafkaSettings` and `RegistrySettings`: immutable, validated on creation, buildable from the environment |
| `src/neon_sage_edge/dns.py` | Process-local hostname-to-IP overrides for the registry |
| `src/neon_sage_edge/registry.py` | The registry client, deserializer factory and `registry_subjects()` |
| `src/neon_sage_edge/decoding.py` | `MessageDecoder`: keys and values through their deserializers, falling back to text instead of failing |
| `src/neon_sage_edge/frames.py` | Table functions: `message_to_row`, `rows_to_frame`, `expand_json`, `to_utc_datetimes`, `time_range`, `per_minute_counts` |
| `src/neon_sage_edge/reading.py` | `MirrorReader`: topics, offsets, `peek`, `read_all`, `read_since`, `read_range`, `watch`, `consumer_groups` |
| `src/neon_sage_edge/logs.py` | `show_notes()`: print the client's notes in a notebook |
| `notebooks/mirror-explorer.ipynb` | The interactive walkthrough |

`MirrorReader` receives its Kafka clients, decoder and clock as constructor arguments, and `MirrorReader.connect()` builds the real ones, so any of them can be swapped out, for example for small fakes when adding tests later.
