# Réponses - Lab 3 Kubernetes : Job et CronJob

Manifests utilisés : `lab-3/` (ceux de l'énoncé). Testé sur minikube (Kubernetes v1.37.0), namespace `lab-job-cronjob`.

## Job vs CronJob

- Job : lance un ou plusieurs pods jusqu'à leur fin avec succès (`restartPolicy: Never` ou `OnFailure`, jamais
  `Always`), relance en cas d'échec, puis s'arrête. Le Job et ses pods restent visibles (statut `Complete`, logs).
- CronJob : crée un Job à chaque créneau d'une expression cron (`minute heure jour mois jour-semaine`).
  Le nom du Job créé est `<cronjob>-<heure prévue en minutes depuis l'epoch>`, ex. `bronze-to-silver-pipeline-29873606`
  pour 13:26 UTC. L'heure est en UTC (pas de `timeZone` défini), les logs affichent 13:26 alors qu'il est 15:26 à Paris.

## 1. Job simple et retry

- `data-import-job` : `Complete 1/1` en 7 s, logs `Starting data import...` → `Import completed!`.
  `kubectl wait --for=condition=complete` rend la main dès la fin du Job (utile dans un script).
- `data-transform-job` échoue 30 % du temps. Relancé plusieurs fois pour voir un échec : au 4e essai, un pod finit en
  `Error` (`ERROR: Validation failed!`), puis le Job crée un **nouveau pod** environ 10 s plus tard (délai de backoff
  exponentiel 10 s, 20 s, 40 s...), qui réussit : `Complete 1/1` en 14 s. Avec `restartPolicy: Never`, chaque essai
  est un pod distinct (on garde les logs de l'échec) ; avec `OnFailure`, le même pod redémarrerait.
- `backoffLimit: 3` : au-delà de 3 échecs le Job passe en `Failed` (`BackoffLimitExceeded`).
  `activeDeadlineSeconds: 60` : le Job est tué après 60 s quoi qu'il arrive (`DeadlineExceeded`), même en plein retry.
  Par défaut (`data-import-job`) le backoff limit est de 6.

## 2. Parallélisme

- `parallelism: 4` + `completions: 4` : les 4 pods démarrent dans la même seconde (13:21:59), le Job est
  `Complete 4/4` en 11 s au lieu d'environ 4 × 7 s en séquentiel.
- Les 4 pods exécutent exactement le même code (mode `NonIndexed`) : rien ne leur dit quel shard traiter.
  Pour un vrai découpage, `completionMode: Indexed` donne à chaque pod la variable `JOB_COMPLETION_INDEX` (0 à 3).

## 3. CronJob

- `bronze-to-silver-pipeline` (`*/2 * * * *`) a créé un Job à 13:24 puis 13:26 (5 s chacun), log :
  `Bronze→Silver ETL started...` → `Completed! 500 records processed`.
- `successfulJobsHistoryLimit: 3` / `failedJobsHistoryLimit: 1` : nombre de Jobs terminés conservés, les plus
  anciens sont supprimés. `concurrencyPolicy: Forbid` : pas de nouveau Job si le précédent tourne encore
  (`Allow` par défaut, `Replace` tue l'ancien).
- `daily-backup` (`0 2 * * *`, 2 h du matin UTC) : jamais déclenché pendant le lab. Test immédiat possible avec
  `kubectl create job backup-manuel --from=cronjob/daily-backup` → log `Backup completed`.
- Autres exemples : `0 * * * *` toutes les heures, `0 2 * * 1` le lundi à 2 h, `0 0 1 * *` le 1er du mois.

## 4. Monitoring

- `kubectl get jobs` (statut, complétions, durée), `describe job` (parallelism, completions, mode, backoff limit,
  `Pods Statuses: 0 Active / 1 Succeeded / 0 Failed`, events `SuccessfulCreate`), `logs -l job-name=<job>`.
- Suspension (`suspend: true`) à 13:27:27 : `SUSPEND True`, aucun Job créé à 13:28 ni à 13:30.
  À la reprise (13:30:50), le CronJob crée tout de suite `bronze-to-silver-pipeline-29873610` (prévu 13:30, créé
  13:30:50) : il rattrape **seulement le dernier créneau manqué**, celui de 13:28 est perdu.
  `startingDeadlineSeconds` limite ce rattrapage.
- Sous PowerShell, le JSON du patch doit être échappé : `-p '{\"spec\":{\"suspend\":true}}'`.

## 5. Scénario pipeline

- `etl-bronze-to-silver` (`*/5 * * * *`) a tourné à 13:30, `BATCH_SIZE=1000` lu depuis `env`. Les 5 lignes de log
  ont le même horodatage car `timestamp` n'est calculé qu'une fois au début du script.
- En vrai, `BATCH_SIZE` et les identifiants S3 viendraient d'une ConfigMap et d'un Secret (lab 2), comme le Job
  `upload-bronze` du lab S3 (`job-upload-bronze.yaml`).

## Teardown

- Supprimer un CronJob supprime aussi ses Jobs (ownerReference), y compris `backup-manuel` créé avec `--from`.
  Namespace supprimé : `kubectl get namespace lab-job-cronjob` → `NotFound`.
