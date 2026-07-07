import { runGoldValuation } from "./runGoldValuation.js";

runGoldValuation()
  .then((result) => {
    console.log("==================================");
    console.log("GOLD VALUATION TEST");
    console.log("==================================");
    console.log(JSON.stringify(result, null, 2));
  })
  .catch((error) => {
    console.error("Gold valuation test failed:");
    console.error(error);
  });