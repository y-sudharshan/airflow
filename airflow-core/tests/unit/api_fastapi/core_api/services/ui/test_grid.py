# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from airflow.api_fastapi.core_api.services.ui.grid import _merge_node_dicts


class TestMergeNodeDicts:
    """Unit tests for _merge_node_dicts function."""

    def test_merge_simple_nodes(self):
        """Test merging simple nodes without children."""
        current = [{"id": "task1", "label": "Task 1", "children": None}]
        new = [{"id": "task2", "label": "Task 2", "children": None}]

        _merge_node_dicts(current, new)

        assert len(current) == 2
        assert current[0]["id"] == "task1"
        assert current[1]["id"] == "task2"

    def test_merge_with_none_new_list(self):
        """Test that merging with None doesn't crash (regression test for issue #61208)."""
        current = [{"id": "task1", "label": "Task 1", "children": None}]
        new = None

        # Should not raise TypeError
        _merge_node_dicts(current, new)

        # Current should remain unchanged
        assert len(current) == 1
        assert current[0]["id"] == "task1"

    def test_merge_task_to_taskgroup_conversion(self):
        """Test merging when a task is converted to a TaskGroup.

        This is the main regression test for issue #61208.
        Old version has task with children=None, new version has TaskGroup with children=[...].
        """
        # Old version: task_a is a simple task
        current = [{"id": "task_a", "label": "Task A", "children": None}]

        # New version: task_a is now a TaskGroup with children
        new = [
            {
                "id": "task_a",
                "label": "Task A",
                "children": [
                    {"id": "task_a.subtask1", "label": "Subtask 1", "children": None},
                    {"id": "task_a.subtask2", "label": "Subtask 2", "children": None},
                ],
            }
        ]

        # Should not raise TypeError: 'NoneType' object is not iterable
        _merge_node_dicts(current, new)

        # Current should now have the new structure (TaskGroup)
        assert len(current) == 1
        assert current[0]["id"] == "task_a"
        # The old node with children=None is kept (not merged into children)

    def test_merge_taskgroup_to_task_conversion(self):
        """Test merging when a TaskGroup is converted to a simple task."""
        # Old version: task_a is a TaskGroup
        current = [
            {
                "id": "task_a",
                "label": "Task A",
                "children": [
                    {"id": "task_a.subtask1", "label": "Subtask 1", "children": None},
                ],
            }
        ]

        # New version: task_a is now a simple task
        new = [{"id": "task_a", "label": "Task A", "children": None}]

        # Should not crash
        _merge_node_dicts(current, new)

        assert len(current) == 1
        assert current[0]["id"] == "task_a"

    def test_merge_nested_children(self):
        """Test merging nodes with nested children."""
        current = [
            {
                "id": "group1",
                "label": "Group 1",
                "children": [
                    {"id": "group1.task1", "label": "Task 1", "children": None},
                ],
            }
        ]

        new = [
            {
                "id": "group1",
                "label": "Group 1",
                "children": [
                    {"id": "group1.task1", "label": "Task 1", "children": None},
                    {"id": "group1.task2", "label": "Task 2", "children": None},
                ],
            }
        ]

        _merge_node_dicts(current, new)

        assert len(current) == 1
        assert current[0]["id"] == "group1"
        # Children should be merged
        assert len(current[0]["children"]) == 2

    def test_merge_empty_children_list(self):
        """Test merging with empty children list."""
        current = [{"id": "task1", "label": "Task 1", "children": []}]
        new = [{"id": "task1", "label": "Task 1", "children": []}]

        _merge_node_dicts(current, new)

        assert len(current) == 1
        assert current[0]["children"] == []

    def test_merge_preserves_unique_nodes(self):
        """Test that merging preserves nodes from both lists."""
        current = [
            {"id": "task1", "label": "Task 1", "children": None},
            {"id": "task2", "label": "Task 2", "children": None},
        ]

        new = [
            {"id": "task3", "label": "Task 3", "children": None},
            {"id": "task4", "label": "Task 4", "children": None},
        ]

        _merge_node_dicts(current, new)

        assert len(current) == 4
        ids = {node["id"] for node in current}
        assert ids == {"task1", "task2", "task3", "task4"}
