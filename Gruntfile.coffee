"use strict"

module.exports = (grunt) ->

  watchFiles =
    js: ["*.js"]
    tests: ["test/**/*.coffee"]
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
    mochaTest:
      all:
        options:
          reporter: 'spec'
          bail: true
          require: 'coffee-script/register'
        src: watchFiles.tests
    apidoc:
      all:
        src: "apidoc_src"
        dest: "apidoc"
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
          "mochaTest"
        ]
      apidoc:
        files: ['server.js', 'package.json']
        tasks: [
          "jshint"
          "apidoc"
        ]
      tests:
        files: watchFiles.tests
        tasks: ["mochaTest"]
      coffee:
        files: watchFiles.gruntfile
        tasks: ["coffeelint", "mochaTest"]

  grunt.registerTask "test", ["coffeelint", "jshint", "mochaTest"]
  grunt.registerTask "default", ["coffeelint", "jshint", "apidoc", "mochaTest", "concurrent"]
