[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=master)](https://travis-ci.org/broadinstitute/rawls) [![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg?branch=master)](https://coveralls.io/r/broadinstitute/rawls?branch=master)

DSDE Workspace Service

TODO document:
* API / Swagger
* setting up Rawls locally

In order to use the Swagger "Try it out" interface on the local server you've just spun up
(following the instructions that don't exist just above this paragraph), you'll need to supply
an authorization cookie.  Here are instructions for doing that in Firefox:
1) Install the Firebug add-on. Go here: http://getfirebug.com/
2) Right-click on a tab (even an empty one works fine) and choose "Inspect Element with Firebug".
   This brings up Firebug's UI at the bottom of the window.
3) You're on the HTML tab.  Click on the Cookies tab.
   You'll see three drop-downs: at the top of the tab: Cookies, Filter, and Default (Accept session cookies).
4) Frob the Cookies drop-down: first choice is "Create Cookie".  Choose it, and you'll get a dialog box pop-up.
5) Fill in the dialog box as follows:
   Name: iPlanetDirectoryPro
   Host: localhost
   Path: /
   Expires: sometime in the far distant future
   Value: test_token (it doesn't really matter)
   Now hit OK to create the cookie.
6) Now surf to http://localhost:8080/ to visit the Swagger interface for your local server.
7) "Try it out!" to your heart's content.

* the workspace model
* the graph DB backend
