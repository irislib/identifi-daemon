"use strict"
module.exports = (grunt) ->
  
  watchFiles =
    js: ["*.js"]
    tests: ["test/**/*.js"]
    gruntfile: ["Gruntfile.coffee"]

  # Show elapsed time at the end
  require("time-grunt")(grunt)
  
  # Load all grunt tasks
  require("load-grunt-tasks")(grunt)

  grunt.initConfig
    coffeelint:
      all:
        src: watchFiles.gruntfile
      options:
        configFile: 'coffeelint.json'
    jshint:
      options:
        jshintrc: ".jshintrc"
        reporter: require("jshint-stylish")
      js:
        src: watchFiles.js
    mochacli:
      all:
        options:
          bail: true
        src: watchFiles.tests
    concurrent:
      dev:
        tasks: ['nodemon', 'watch']
        options:
          logConcurrentOutput: true
    nodemon:
      dev:
        script: 'server.js'
        options:
          nodeArgs: [ '--debug' ]
          ext: 'js,html'
          watch: watchFiles.js
    watch:
      js:
        files: watchFiles.js
        tasks: [
          "jshint"
          "mochacli"
        ]
      tests:
        files: watchFiles.tests
        tasks: ["mochacli"]
      coffee:
        files: watchFiles.gruntfile
        tasks: ["coffeelint", "mochacli"]

  grunt.registerTask "test", ["coffeelint", "jshint", "mochacli"]
  grunt.registerTask "default", ["coffeelint", "jshint", "mochacli", "concurrent"]
