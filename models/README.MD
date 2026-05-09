# models/

Trained Spark ML pipelines from Stage 3. Each subfolder is a serialized
`PipelineModel` — that's Spark's native format, a directory with a
metadata file and Parquet stages, not a single binary.

We train three classifiers because the rubric requires it:

- `rf/` — Random Forest. Best of a 27-cell grid over `numTrees`,
  `maxDepth`, `maxBins`.
- `svm/` — Linear SVC. Best of `regParam × maxIter × tol` (also 27 cells).
- `nb/` — Naive Bayes. Best of `smoothing × modelType × scaler.max`.

Each pipeline includes the full preprocessing chain (assembler + scaler +
classifier), so you don't need a separate feature-extraction step on
inference — load the model and call `.transform()` on any DataFrame
that has the 18 feature columns (see `feature_cols` in
`scripts/model.py`).

Loading one back:

```python
from pyspark.ml import PipelineModel
rf = PipelineModel.load("models/rf")
rf.transform(test_df).select("repo_id", "prediction").show()
```

Picking which is "best" — `output/evaluation.csv` has the comparison.
At the registered pseudo-label thresholds the positive class is ~0.08%,
so PR-AUC is the metric that matters; AUROC is misleading at that
imbalance.
