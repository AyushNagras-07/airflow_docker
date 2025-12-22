Directed: 

This signifies that there is a clear, one-way flow of execution between tasks. Tasks have defined dependencies, meaning some tasks must complete successfully before others can begin.

Acyclic: 
This means there are no cycles within the graph. A task cannot depend on itself, either directly or indirectly through a chain of other tasks. This prevents infinite loops and ensures the workflow has a clear start and end.

Graph: 
The workflow is represented as a collection of nodes (tasks) and edges (dependencies) that illustrate the relationships and order of execution.

✔ Logical Date

“Which date this DAG run represents”

✔ ds

“Logical date formatted as YYYY-MM-DD string”

✔ run_id

“Unique ID of this specific DAG run instance”

One logical date = one output

    No matter:

    how many retries

    how many failures

    how many reruns