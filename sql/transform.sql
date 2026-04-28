MERGE `TU_PROJECT_ID.INTEGRATION.integration_prueba_tecnica` T
USING (
    SELECT
        id,
        userId,
        title,
        body,
        CURRENT_DATE() AS execution_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY id
            ) AS rn
        FROM `TU_PROJECT_ID.SANDBOX_PRUEBA_API.posts`
    )
    WHERE rn = 1
) S
ON T.id = S.id

WHEN MATCHED THEN
UPDATE SET
    userId = S.userId,
    title = S.title,
    body = S.body,
    execution_date = S.execution_date

WHEN NOT MATCHED THEN
INSERT (
    id,
    userId,
    title,
    body,
    execution_date
)
VALUES (
    S.id,
    S.userId,
    S.title,
    S.body,
    S.execution_date
);