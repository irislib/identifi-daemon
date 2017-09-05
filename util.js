module.exports = {
  removeDuplicates: function removeDuplicates(arr) {
    const seen = {};
    return arr.filter(item => (seen.hasOwnProperty(item) ? false : (seen[item] = true)));
  },
};
