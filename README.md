[![Build Status](https://travis-ci.org/broadinstitute/rawls.svg?branch=master)](https://travis-ci.org/broadinstitute/rawls) [![Coverage Status](https://coveralls.io/repos/broadinstitute/rawls/badge.svg?branch=master)](https://coveralls.io/r/broadinstitute/rawls?branch=master)

#DSDE Workspace Service

### API / Swagger

### Setting up Rawls locally

Get a recent copy of rawls.conf, the master configuration file (from dsde-jenkins.broadinstitute.org if
  you have admin priveleges, or from a teammate if you don't).  Put it in /etc/rawls.conf.  Probably
  a good idea to make it readable by you alone, since it contains secrets.  

Cd to your rawls git workarea, and type sbt.  
  After a few preliminaries, you should see sbt's command prompt.  
  Type the command "re-start", and your server will spin up locally.  

You can exercise it using a browser via the Swagger interface at http://localhost:8080/.  

You can make code changes, and just type "re-start" again to re-run the service.  sbt will
  recompile the code and package it up, and re-start the server for you.  

Type exit (or ^C) when you're done and you want to kill sbt and the local server.  

In order to use the Swagger "Try it out" interface on the local server you've just spun up
you'll need to supply an authorization cookie.  Here are instructions for doing that in Firefox:  

1. Install the Firebug add-on. Go here: http://getfirebug.com/
2. Right-click on a tab (even an empty one works fine) and choose "Inspect Element with Firebug".
   This brings up Firebug's UI at the bottom of the window.
3. You're on the HTML tab.  Click on the Cookies tab.
   You'll see three drop-downs at the top of the tab: Cookies, Filter, and Default (Accept session cookies).
4. Frob the Cookies drop-down: first choice is "Create Cookie".  Choose it, and you'll get a dialog box pop-up.
5. Fill in the dialog box as follows:
   Name: iPlanetDirectoryPro
   Host: localhost
   Path: /
   Expires: sometime in the far distant future
   Value: test_token (it doesn't really matter)
   Now hit OK to create the cookie.
6. Now surf to http://localhost:8080/ to visit the Swagger interface for your local server.
7. "Try it out!" to your heart's content.  

In chrome, navigate to http://localhost:8080 then enter this into the address bar: javascript:document.cookie="iPlanetDirectoryPro=myCookieValue"

### The workspace model

### The graph DB backend
 
