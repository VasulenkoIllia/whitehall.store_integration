function roundUpTo10(value) {
  return Math.ceil(value / 10) * 10;
}

function calculateFinalPrice(basePrice, markupPercent) {
  const price = Number(basePrice);
  if (!Number.isFinite(price) || price <= 0) {
    return null;
  }

  let priceWithMarkup = price;
  if (markupPercent > 0) {
    priceWithMarkup = price * (1 + markupPercent / 100);
    if (priceWithMarkup - price < 500) {
      priceWithMarkup = price + 500;
    }
  }

  return roundUpTo10(priceWithMarkup);
}

module.exports = {
  calculateFinalPrice
};
