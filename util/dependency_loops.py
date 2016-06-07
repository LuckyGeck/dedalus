from typing import Dict, List, Optional, Iterable, List, Tuple


def detect_loop(deps: Dict[str, List[str]]) -> Optional[Iterable[str]]:
    if len(deps) == 0:
        return None
    # visited - Maps visited task name to bool (True - if name is currently in cur_stack)
    visited = dict()  # type: Dict[str, bool]
    for key in deps.keys():
        if key in visited:
            continue
        # cur_stack - current DFS stack. Contains pairs (task_name, idx_of_next_child)
        cur_stack = [(key, 0)]  # type: List[Tuple[str, int]]
        visited[key] = True
        while cur_stack:
            top_name, top_child_idx = cur_stack.pop()
            top_children = deps.get(top_name, list())
            if len(top_children) <= top_child_idx:
                visited[top_name] = False
                continue
            cur_stack.append((top_name, top_child_idx + 1))
            child_name = top_children[top_child_idx]
            child_seen = visited.get(child_name)
            if child_seen is not None and child_seen is False:  # child has been visited on previous DFS pass
                continue
            cur_stack.append((child_name, 0))
            if child_seen is True:  # child is in stack - loop found
                return (k for k, v in cur_stack)
            if child_seen is None:  # never visited this task before
                visited[child_name] = True
    return None


if __name__ == '__main__':
    assert detect_loop({'1': ['1']})
    assert not detect_loop({'1': ['3', '2'], '2': ['3']})
    assert detect_loop({'1': ['3', '2'], '2': ['3'], 'a': ['b'], 'b': ['1', 'a']})
    assert detect_loop({'1': ['2'], '2': ['3'], '3': ['4'], '4': ['3']})
