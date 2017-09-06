module.exports = {
  removeDuplicates: function removeDuplicates(arr) {
    const seen = {};
    return arr.filter((item) => {
      if (Object.prototype.hasOwnProperty.call(seen, item)) {
        return false;
      }
      seen[item] = true;
      return true;
    });
  },
};
