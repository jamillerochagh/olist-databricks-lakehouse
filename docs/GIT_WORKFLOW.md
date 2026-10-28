# Practice Git with this project

Git tracks file history. GitHub hosts a repository and supports collaboration.
Complete this workflow hands-on before listing basic Git skills on a CV.

## Create the initial local history

Extract the code archive and open a terminal in `olist-databricks-lakehouse`.
If the extracted folder is not already a repository, initialize it:

```bash
git init -b main
```

Configure a name and the private no-reply email shown in your GitHub email settings:

```bash
git config user.name "Jamille Ghazaleh"
git config user.email "<your GitHub no-reply email>"
```

Stage the code and documentation, review the selection, and create a commit:

```bash
git add .
git diff --cached --stat
git status
git commit -m "Build validated Olist lakehouse and dashboard"
```

The project ignore rules exclude raw CSVs, archives, local environments and
notebook checkpoints. The repository contains reproducible code and source attribution.

## Create a remote repository

Create an empty private GitHub repository named `olist-databricks-lakehouse`.
Use its actual URL in place of the placeholder below. Do not initialize the
remote with a separate README if the local history already includes this one.

```bash
git remote add origin <your-repository-URL>
git push -u origin main
```

Private visibility allows review before approving public portfolio access.

## Make and review a small change

Create a branch, make a real documentation improvement, then inspect and commit it:

```bash
git switch -c docs/explain-metrics
git diff
git add docs/GOLD_METRICS.md
git commit -m "Clarify the delivery metric definition"
git push -u origin docs/explain-metrics
```

Open a pull request in GitHub, review the change, and merge it. Then update locally:

```bash
git switch main
git pull --ff-only
git log --oneline -5
```

Do not create an empty change just to produce a commit. Be able to explain
staging, a commit, a branch, a push and a pull request using this project.

## CV wording after practice

`Git/GitHub: basic version control with commits, branches, push/pull and pull requests.`

Databricks can be described as project experience once the notebook/dashboard
workflow has been run and understood. Add a certification to the credentials
section only after earning it, using its exact title and credential link.

Official guides:

- https://docs.github.com/en/get-started/using-git/about-git
- https://docs.github.com/en/repositories/creating-and-managing-repositories/quickstart-for-repositories
