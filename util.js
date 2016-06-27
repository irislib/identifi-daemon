module.exports = {
  removeDuplicates: function removeDuplicates(arr) {
    var seen = {};
    return arr.filter(function(item) {
      return seen.hasOwnProperty(item) ? false : (seen[item] = true);
    });
  }
};
