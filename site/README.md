<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

## Xskipper Site

This directory contains the source for the Xskipper site.

* Site structure is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Links use bare page names: `[link text](target-page)`

### Installation 

The site is built using mkdocs using [Material theme](https://squidfunk.github.io/mkdocs-material/getting-started/).\
To install mkdocs and the theme, run:

```
pip install mkdocs-material
pip install mkdocs-redirects
```

### Local Changes

To see changes locally before committing, use mkdocs to run a local server from this directory.

```
mkdocs serve
```

### Publishing

Changes to the site are published automatically on merge to master using GitHub Actions.

In case you want to deploy the site manually, you can publish the site with this command:

```
mkdocs gh-deploy
```

This assumes that the Xskipper remote is named `xskipper` and will push to the `xskipper-site` branch. To use a different remote add -r <remote-name>.