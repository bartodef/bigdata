# Lab 2.3 Kubernetes : Job et CronJob

J'ai utilisé les fichiers de l'énoncé dans `lab-3/`. Testé sur minikube dans le namespace `lab-job-cronjob`.

## Job ou CronJob

Un Job lance un ou plusieurs pods jusqu'à ce qu'ils finissent bien. Si ça échoue il relance. Ensuite il s'arrête. Le `restartPolicy` est `Never` ou `OnFailure` mais jamais `Always`. Le Job et ses pods restent visibles après (statut Complete et logs).

Un CronJob crée un Job à chaque fois que l'horaire cron tombe. Le Job créé s'appelle par exemple `bronze-to-silver-pipeline-29873606`. Le nombre c'est l'heure prévue en minutes. L'heure est en UTC : les logs disent 13:26 alors qu'il est 15:26 à Paris.

## 1. Job simple et retry

`data-import-job` passe en Complete 1/1 en 7 s. Les logs disent `Starting data import...` puis `Import completed!`. `kubectl wait --for=condition=complete` attend la fin du Job, c'est pratique dans un script.

`data-transform-job` échoue 30 % du temps. J'ai dû le relancer plusieurs fois pour voir un échec. Au 4e essai un pod est en `Error` (`ERROR: Validation failed!`). Environ 10 s après le Job crée un nouveau pod qui réussit. Le délai augmente à chaque échec (10 s puis 20 s puis 40 s).

Avec `restartPolicy: Never` chaque essai est un nouveau pod donc on garde les logs de l'échec. Avec `OnFailure` c'est le même pod qui redémarre.

`backoffLimit: 3` : après 3 échecs le Job passe en Failed.
`activeDeadlineSeconds: 60` : le Job est arrêté après 60 s même s'il est en train de réessayer.
Par défaut le backoffLimit vaut 6.

## 2. Parallélisme

Avec `parallelism: 4` et `completions: 4` les 4 pods démarrent en même temps (13:21:59). Le Job finit en 11 s au lieu d'environ 28 s si c'était l'un après l'autre.

Les 4 pods font exactement la même chose. Rien ne leur dit quelle partie des données traiter. Pour ça il faut `completionMode: Indexed`, chaque pod reçoit alors un numéro dans `JOB_COMPLETION_INDEX` (de 0 à 3).

## 3. CronJob

`bronze-to-silver-pipeline` (`*/2 * * * *`) a créé un Job à 13:24 et un autre à 13:26. Le log dit `Completed! 500 records processed`.

- `successfulJobsHistoryLimit: 3` et `failedJobsHistoryLimit: 1` : le nombre de Jobs terminés qu'on garde
- `concurrencyPolicy: Forbid` : pas de nouveau Job si l'ancien tourne encore (par défaut c'est `Allow`)

`daily-backup` (`0 2 * * *`) tourne à 2h du matin donc il ne s'est jamais lancé pendant le lab. Pour le tester tout de suite j'ai fait `kubectl create job backup-manuel --from=cronjob/daily-backup` et le log dit `Backup completed`.

Autres exemples :

- `0 * * * *` toutes les heures
- `0 2 * * 1` le lundi à 2h
- `0 0 1 * *` le 1er du mois

## 4. Monitoring

J'ai utilisé `kubectl get jobs`, `kubectl describe job` et `kubectl logs -l job-name=<job>`.

J'ai suspendu le CronJob (`suspend: true`) à 13:27. Il n'a rien créé à 13:28 ni à 13:30. Quand je l'ai relancé à 13:30:50 il a tout de suite créé le Job de 13:30. Il rattrape seulement le dernier créneau raté. Celui de 13:28 est perdu.

Sous PowerShell il faut échapper les guillemets du patch : `-p '{\"spec\":{\"suspend\":true}}'`.

## 5. Scénario pipeline

`etl-bronze-to-silver` (`*/5 * * * *`) a tourné à 13:30 avec `BATCH_SIZE=1000`. Les 5 lignes de log ont la même heure car le timestamp est calculé une seule fois au début du script.

Dans un vrai projet `BATCH_SIZE` et les identifiants S3 viendraient d'une ConfigMap et d'un Secret comme dans le lab 2.2.

## Nettoyage

Quand on supprime un CronJob ses Jobs sont supprimés aussi. Après la suppression du namespace `kubectl get namespace lab-job-cronjob` renvoie NotFound.
