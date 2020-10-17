const gulp = require('gulp');
var run = require('gulp-run-command').default

 
gulp.task('runProducer', run('node .\\src\\producer.js'))

gulp.task('runConsumer',run('node ./src/consumer.js'))
