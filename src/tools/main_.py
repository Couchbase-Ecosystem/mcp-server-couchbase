

import json
from typing import Any

schema =[
  [
    {
      "#docs": 2,
      "$schema": "http://json-schema.org/draft-06/schema",
      "Flavor": "",
      "properties": {
        "company": {
          "#docs": 2,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "departments": {
              "#docs": 2,
              "%docs": 100,
              "items": {
                "#docs": 4,
                "$schema": "http://json-schema.org/draft-06/schema",
                "Flavor": "",
                "properties": {
                  "name": {
                    "#docs": 4,
                    "%docs": 100,
                    "nestingDepth": 3,
                    "type": "string"
                  },
                  "teams": {
                    "#docs": 4,
                    "%docs": 100,
                    "items": {
                      "#docs": 1,
                      "$schema": "http://json-schema.org/draft-06/schema",
                      "Flavor": "",
                      "properties": {
                        "projects": {
                          "#docs": 1,
                          "%docs": 100,
                          "items": {
                            "#docs": 1,
                            "$schema": "http://json-schema.org/draft-06/schema",
                            "properties": {
                              "tasks": {
                                "items": {
                                  "#docs": 2,
                                  "$schema": "http://json-schema.org/draft-06/schema",
                                  "properties": {
                                    "description": {
                                      "nestingDepth": 9,
                                      "type": "string"
                                    },
                                    "status": {
                                      "nestingDepth": 9,
                                      "type": "string"
                                    },
                                    "subtasks": {
                                      "items": {
                                        "#docs": 3,
                                        "$schema": "http://json-schema.org/draft-06/schema",
                                        "properties": {
                                          "completed": {
                                            "nestingDepth": 11,
                                            "type": "boolean"
                                          },
                                          "subtask_id": {
                                            "nestingDepth": 11,
                                            "type": "string"
                                          },
                                          "title": {
                                            "nestingDepth": 11,
                                            "type": "string"
                                          }
                                        },
                                        "type": "object"
                                      },
                                      "maxItems": 2,
                                      "minItems": 1,
                                      "nestingDepth": 9,
                                      "sampleSize": 0,
                                      "type": "array"
                                    },
                                    "task_id": {
                                      "nestingDepth": 9,
                                      "type": "number"
                                    }
                                  },
                                  "type": "object"
                                },
                                "maxItems": 2,
                                "minItems": 2,
                                "nestingDepth": 7,
                                "sampleSize": 0,
                                "type": "array"
                              },
                              "title": {
                                "nestingDepth": 7,
                                "type": "string"
                              }
                            },
                            "type": "object"
                          },
                          "maxItems": 1,
                          "minItems": 1,
                          "nestingDepth": 5,
                          "sampleSize": 0,
                          "type": "array"
                        }
                      },
                      "type": "object"
                    },
                    "maxItems": 1,
                    "minItems": 1,
                    "nestingDepth": 3,
                    "sampleSize": 0,
                    "type": "array"
                  }
                },
                "type": "object"
              },
              "maxItems": 2,
              "minItems": 2,
              "nestingDepth": 1,
              "sampleSize": 0,
              "samples": [
                [
                  {
                    "name": "Engineering",
                    "teams": [
                      {
                        "projects": [
                          {
                            "tasks": [
                              {
                                "description": "Train edge vision model",
                                "status": "in-progress",
                                "subtasks": [
                                  {
                                    "completed": False,
                                    "subtask_id": "1a",
                                    "title": "Collect dataset"
                                  },
                                  {
                                    "completed": True,
                                    "subtask_id": "1b",
                                    "title": "Data augmentation"
                                  }
                                ],
                                "task_id": 1
                              },
                              {
                                "description": "Optimize inference runtime",
                                "status": "pending",
                                "subtasks": [
                                  {
                                    "completed": False,
                                    "subtask_id": "2a",
                                    "title": "Quantization test"
                                  }
                                ],
                                "task_id": 2
                              }
                            ],
                            "title": "Edge AI Vision"
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "name": "Marketing",
                    "teams": [
                      {
                        "name": "Digital Campaigns",
                        "projects": [
                          {
                            "tasks": [
                              {
                                "status": "completed",
                                "subtasks": [
                                  {
                                    "title": "Prepare creatives"
                                  },
                                  {
                                    "completed": True,
                                    "subtask_id": "10b",
                                    "title": "Schedule posts"
                                  }
                                ],
                                "task_id": 10
                              }
                            ],
                            "title": "AI Product Launch"
                          }
                        ]
                      }
                    ]
                  }
                ],
                [
                  {
                    "name": "Engineering",
                    "teams": [
                      {
                        "name": "AI Research",
                        "projects": [
                          {
                            "tasks": [
                              {
                                "description": "Train edge vision model",
                                "status": "in-progress",
                                "subtasks": [
                                  {
                                    "completed": False,
                                    "subtask_id": "1a",
                                    "title": "Collect dataset"
                                  },
                                  {
                                    "completed": True,
                                    "subtask_id": "1b",
                                    "title": "Data augmentation"
                                  }
                                ],
                                "task_id": 1
                              },
                              {
                                "description": "Optimize inference runtime",
                                "status": "pending",
                                "subtasks": [
                                  {
                                    "completed": False,
                                    "subtask_id": "2a",
                                    "title": "Quantization test"
                                  }
                                ],
                                "task_id": 2
                              }
                            ],
                            "title": "Edge AI Vision"
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "name": "Marketing",
                    "teams": [
                      {
                        "name": "Digital Campaigns",
                        "projects": [
                          {
                            "tasks": [
                              {
                                "description": "Social media rollout",
                                "status": "completed",
                                "subtasks": [
                                  {
                                    "completed": True,
                                    "subtask_id": "10a",
                                    "title": "Prepare creatives"
                                  },
                                  {
                                    "completed": True,
                                    "subtask_id": "10b",
                                    "title": "Schedule posts"
                                  }
                                ],
                                "task_id": 10
                              }
                            ],
                            "title": "AI Product Launch"
                          }
                        ]
                      }
                    ]
                  }
                ]
              ],
              "type": "array"
            },
            "name": {
              "#docs": 2,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "TechNova"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "departments": [
                {
                  "name": "Engineering",
                  "teams": [
                    {
                      "projects": [
                        {
                          "tasks": [
                            {
                              "description": "Train edge vision model",
                              "status": "in-progress",
                              "subtasks": [
                                {
                                  "completed": False,
                                  "subtask_id": "1a",
                                  "title": "Collect dataset"
                                },
                                {
                                  "completed": True,
                                  "subtask_id": "1b",
                                  "title": "Data augmentation"
                                }
                              ],
                              "task_id": 1
                            },
                            {
                              "description": "Optimize inference runtime",
                              "status": "pending",
                              "subtasks": [
                                {
                                  "completed": False,
                                  "subtask_id": "2a",
                                  "title": "Quantization test"
                                }
                              ],
                              "task_id": 2
                            }
                          ],
                          "title": "Edge AI Vision"
                        }
                      ]
                    }
                  ]
                },
                {
                  "name": "Marketing",
                  "teams": [
                    {
                      "name": "Digital Campaigns",
                      "projects": [
                        {
                          "tasks": [
                            {
                              "status": "completed",
                              "subtasks": [
                                {
                                  "title": "Prepare creatives"
                                },
                                {
                                  "completed": True,
                                  "subtask_id": "10b",
                                  "title": "Schedule posts"
                                }
                              ],
                              "task_id": 10
                            }
                          ],
                          "title": "AI Product Launch"
                        }
                      ]
                    }
                  ]
                }
              ],
              "name": "TechNova"
            },
            {
              "departments": [
                {
                  "name": "Engineering",
                  "teams": [
                    {
                      "name": "AI Research",
                      "projects": [
                        {
                          "tasks": [
                            {
                              "description": "Train edge vision model",
                              "status": "in-progress",
                              "subtasks": [
                                {
                                  "completed": False,
                                  "subtask_id": "1a",
                                  "title": "Collect dataset"
                                },
                                {
                                  "completed": True,
                                  "subtask_id": "1b",
                                  "title": "Data augmentation"
                                }
                              ],
                              "task_id": 1
                            },
                            {
                              "description": "Optimize inference runtime",
                              "status": "pending",
                              "subtasks": [
                                {
                                  "completed": False,
                                  "subtask_id": "2a",
                                  "title": "Quantization test"
                                }
                              ],
                              "task_id": 2
                            }
                          ],
                          "title": "Edge AI Vision"
                        }
                      ]
                    }
                  ]
                },
                {
                  "name": "Marketing",
                  "teams": [
                    {
                      "name": "Digital Campaigns",
                      "projects": [
                        {
                          "tasks": [
                            {
                              "description": "Social media rollout",
                              "status": "completed",
                              "subtasks": [
                                {
                                  "completed": True,
                                  "subtask_id": "10a",
                                  "title": "Prepare creatives"
                                },
                                {
                                  "completed": True,
                                  "subtask_id": "10b",
                                  "title": "Schedule posts"
                                }
                              ],
                              "task_id": 10
                            }
                          ],
                          "title": "AI Product Launch"
                        }
                      ]
                    }
                  ]
                }
              ],
              "name": "TechNova"
            }
          ],
          "type": "object"
        },
        "~meta": {
          "#docs": 2,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "id": {
              "#docs": 2,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "doc1",
                "doc4"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "id": "doc1"
            },
            {
              "id": "doc4"
            }
          ],
          "type": "object"
        }
      },
      "type": "object"
    },
    {
      "#docs": 1,
      "$schema": "http://json-schema.org/draft-06/schema",
      "Flavor": "",
      "properties": {
        "company": {
          "#docs": 1,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "departments": {
              "#docs": 1,
              "%docs": 100,
              "items": {
                "type": "number"
              },
              "maxItems": 1,
              "minItems": 1,
              "nestingDepth": 1,
              "sampleSize": 0,
              "samples": [
                [
                  5
                ]
              ],
              "type": "array"
            },
            "name": {
              "#docs": 1,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                5555
              ],
              "type": "number"
            }
          },
          "samples": [
            {
              "departments": [
                5
              ],
              "name": 5555
            }
          ],
          "type": "object"
        },
        "temp": {
          "#docs": 1,
          "%docs": 100,
          "items": [
            {
              "type": "number"
            },
            {
              "type": "string"
            },
            {
              "#docs": 1,
              "$schema": "http://json-schema.org/draft-06/schema",
              "Flavor": "`cc` = 1",
              "properties": {
                "cc": {
                  "#docs": 1,
                  "%docs": 100,
                  "nestingDepth": 2,
                  "samples": [
                    1
                  ],
                  "type": "number"
                }
              },
              "type": "object"
            }
          ],
          "maxItems": 3,
          "minItems": 3,
          "nestingDepth": 0,
          "sampleSize": 0,
          "samples": [
            [
              1,
              "232",
              {
                "cc": 1
              }
            ]
          ],
          "type": "array"
        },
        "~meta": {
          "#docs": 1,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "id": {
              "#docs": 1,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "doc3"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "id": "doc3"
            }
          ],
          "type": "object"
        }
      },
      "type": "object"
    },
    {
      "#docs": 2,
      "$schema": "http://json-schema.org/draft-06/schema",
      "Flavor": "",
      "properties": {
        "address": {
          "#docs": 2,
          "%docs": 100,
          "nestingDepth": 0,
          "samples": [
            "test",
            "test2"
          ],
          "type": "string"
        },
        "phno": {
          "#docs": 2,
          "%docs": 100,
          "nestingDepth": 0,
          "samples": [
            56567567567567,
            904323434345345
          ],
          "type": "number"
        },
        "~meta": {
          "#docs": 2,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "id": {
              "#docs": 2,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "doc5",
                "doc6"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "id": "doc5"
            },
            {
              "id": "doc6"
            }
          ],
          "type": "object"
        }
      },
      "type": "object"
    },
    {
      "#docs": 1,
      "$schema": "http://json-schema.org/draft-06/schema",
      "Flavor": "",
      "properties": {
        "company": {
          "#docs": 1,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "name": {
              "#docs": 1,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "TechNova"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "name": "TechNova"
            }
          ],
          "type": "object"
        },
        "departments": {
          "#docs": 1,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "name": {
              "#docs": 1,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "Arts"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "name": "Arts"
            }
          ],
          "type": "object"
        },
        "temp": {
          "#docs": 1,
          "%docs": 100,
          "items": [
            {
              "type": "number"
            },
            {
              "type": "string"
            }
          ],
          "maxItems": 2,
          "minItems": 2,
          "nestingDepth": 0,
          "sampleSize": 0,
          "samples": [
            [
              2,
              "532"
            ]
          ],
          "type": "array"
        },
        "~meta": {
          "#docs": 1,
          "%docs": 100,
          "nestingDepth": 0,
          "properties": {
            "id": {
              "#docs": 1,
              "%docs": 100,
              "nestingDepth": 1,
              "samples": [
                "doc2"
              ],
              "type": "string"
            }
          },
          "samples": [
            {
              "id": "doc2"
            }
          ],
          "type": "object"
        }
      },
      "type": "object"
    }
  ]
]


def _compact_property(prop_name: str, prop_info: dict[str, Any]) -> dict[str, Any]:
    """Compact a single INFER property into an LLM-friendly dict.

    Keeps: field name, type, %docs (presence percentage), samples,
    and recursively compacts nested objects and array items.

    To keep the prompt size manageable, ``samples`` are only included on
    *leaf* fields (string, number, boolean, null).  For object and array
    fields we already recurse into their ``properties`` / ``items``, so
    the full-document samples would be redundant.
    """
    entry: dict[str, Any] = {"field": prop_name}
    field_type = prop_info.get("type")

    if field_type is not None:
        entry["type"] = field_type

    if "%docs" in prop_info:
        entry["%docs"] = prop_info["%docs"]

    # ── Nested object properties ─────────────────────────────────────
    has_nested = False
    if "properties" in prop_info:
        has_nested = True
        entry["properties"] = [
            _compact_property(k, v)
            for k, v in prop_info["properties"].items()
        ]

    # ── Array items ──────────────────────────────────────────────────
    items_raw = prop_info.get("items")
    if items_raw is not None:
        if isinstance(items_raw, dict):
            # Homogeneous array — items is a single object schema
            has_nested = True
            if "properties" in items_raw:
                entry["items"] = [
                    _compact_property(k, v)
                    for k, v in items_raw["properties"].items()
                ]
            else:
                # Scalar array (e.g. items: {type: "number"})
                entry["item_type"] = items_raw.get("type")
        elif isinstance(items_raw, list):
            # Tuple-style (positional) array — items is a list of schemas
            has_nested = True
            compact_items: list[dict[str, Any]] = []
            for idx, item_schema in enumerate(items_raw):
                if isinstance(item_schema, dict) and "properties" in item_schema:
                    # Object element at this position
                    compact_items.append({
                        "index": idx,
                        "type": item_schema.get("type", "object"),
                        "properties": [
                            _compact_property(k, v)
                            for k, v in item_schema["properties"].items()
                        ],
                    })
                elif isinstance(item_schema, dict):
                    compact_items.append({
                        "index": idx,
                        "type": item_schema.get("type"),
                    })
            if compact_items:
                entry["items"] = compact_items

        if "minItems" in prop_info:
            entry["minItems"] = prop_info["minItems"]
        if "maxItems" in prop_info:
            entry["maxItems"] = prop_info["maxItems"]

    # ── Samples — only on leaf types to avoid bloat ──────────────────
    if "samples" in prop_info and not has_nested:
        entry["samples"] = prop_info["samples"]

    return entry


def _compact_infer_schema(infer_result: list[Any]) -> list[dict[str, Any]]:
    """Compact the raw INFER output into an LLM-friendly representation.

    INFER returns ``[[flavor_1, flavor_2, ...]]`` — an array wrapping an
    array of schema "flavors".  Each flavor is a JSON-Schema-like object
    with a ``properties`` dict where every property already carries:

    - ``type`` — data type (string, number, boolean, array, object, null)
    - ``#docs`` / ``%docs`` — how many / what % of sampled docs contain it
    - ``samples`` — example values drawn from the sample population

    This helper keeps the useful bits (field, type, %docs, samples) and
    recursively compacts nested objects and array items, stripping verbose
    JSON-Schema metadata the downstream agent doesn't need.

    Samples are only preserved on leaf-level fields to avoid massive
    redundancy — for compound types the nested schema already conveys
    the structure.
    """
    # INFER returns [[flavor, ...]] — unwrap the outer array
    flavors = infer_result[0] if infer_result else []
    if isinstance(flavors, dict):
        # Single flavor returned without inner list wrapping
        flavors = [flavors]

    compacted: list[dict[str, Any]] = []
    for flavor in flavors:
        flavor_entry: dict[str, Any] = {}
        if "Flavor" in flavor:
            flavor_entry["flavor"] = flavor["Flavor"]
        if "#docs" in flavor:
            flavor_entry["docs_sampled"] = flavor["#docs"]

        props = flavor.get("properties", {})
        flavor_entry["fields"] = [
            _compact_property(k, v)
            for k, v in props.items()
            if k != "~meta"  # skip internal meta field
        ]
        compacted.append(flavor_entry)

    return compacted



if __name__ == "__main__":
    compacted = _compact_infer_schema(schema)
    print(json.dumps(compacted, indent=2))

