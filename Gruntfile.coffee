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
    eslint:
      options:
        eslintrc: ".eslintrc"
        reporter: require("eslint-stylish")
      js:
        src: watchFiles.js
    mochaTest:
      all:
        options:
          reporter: 'spec'
          bail: false
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
          "eslint"
          "mochaTest"
        ]
      apidoc:
        files: ['server.js', 'package.json']
        tasks: [
          "eslint"
          "apidoc"
        ]
      tests:
        files: watchFiles.tests
        tasks: ["mochaTest"]
      coffee:
        files: watchFiles.gruntfile
        tasks: ["coffeelint", "mochaTest"]

  grunt.registerTask "test", ["coffeelint", "eslint", "mochaTest"]
  grunt.registerTask "default", ["coffeelint", "eslint", "apidoc", "mochaTest", "concurrent"]
