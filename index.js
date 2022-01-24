import fs from "fs";
import csv from "csv-parser";
import validatorEmail from "email-validator";
import googlePhone from "google-libphonenumber";
const { PhoneNumberFormat: PNF, PhoneNumberUtil: phoneUtil } = googlePhone;

const readInputFile = () => {
  return new Promise((resolve, reject) => {
    try {
      const data = [];
      fs.createReadStream("./entrada.csv")
        .pipe(csv())
        .on("headers", (headers) => {
          const indexDuplicateHeader = [];

          headers.forEach((header, index) => {
            const results = headers.filter((h) => h === header);
            if (results.length > 1) indexDuplicateHeader.push(index);
          });

          indexDuplicateHeader.forEach(
            (index) => (headers[index] = `${headers[index]}_duplicate_${index}`)
          );
        })
        .on("data", (res) => data.push(res))
        .on("end", () => resolve(data));
    } catch (error) {
      reject(error);
    }
  });
};

const formatData = async () => {
  const formattedData = [];
  const unformattedData = await readInputFile();

  unformattedData.forEach((data) => {
    const newObj = { addresses: [] };

    Object.entries(data).forEach(([keys, value]) => {
      const [key] = keys.split(" ");
      const tags = keys.split(" ").splice(1);

      const validValues = validateValues(key, value);
      if (!validValues) return;

      if (tags.length) {
        validValues.forEach((v) => {
          if (!v) return;
          newObj.addresses.push({
            type: key,
            tags: tags,
            address: v,
          });
        });
      } else {
        const newValue = validateValues(
          key === "group" ? key : "validateBoolean",
          value
        );
        Reflect.set(newObj, key, newValue);
      }
    });

    formattedData.push(newObj);
  });

  const data = validateDuplicates(formattedData);
  createOutputFile(data);
};

const createOutputFile = (file) => {
  try {
    fs.writeFileSync("output.json", JSON.stringify(file), "utf-8");
  } catch (err) {
    // console.log("err", err);
    return;
  }
};

const validateValues = (type, value) => {
  try {
    if (type === "phone") {
      const isValidNumber = value.replace(/\D/g, "").length > 8 ? true : false;
      if (!isValidNumber) return [];

      const number = phoneUtil.getInstance().parseAndKeepRawInput(value, "BR");
      const formattedNumber = phoneUtil.getInstance().format(number, PNF.E164);
      return [formattedNumber.replace("+", "")];
    }

    if (type === "email") {
      const values = value.split(/[/,\s]/);
      const validValues = [];

      values.forEach((value) => {
        if (validatorEmail.validate(value.trim())) {
          validValues.push(value);
        }
      });
      return validValues;
    }

    if (type === "group") {
      const validGroups = value.split(/[/,/]/).map((value) => value.trim());
      return [...new Set(validGroups)];
    }

    if (type === "validateBoolean") {
      if (value === "yes" || value === "1") return true;
      else if (!value || value === "0" || value === "no") return false;
      else return value;
    }

    return [value];
  } catch (error) {
    // console.log("error", error);
    return;
  }
};

const validateDuplicates = (data) => {
  const newData = [];

  const existsDuplicateHeader = Object.keys(data[0]).some((key) =>
    key.search("_duplicate")
  );

  data.forEach((item) => {
    if (existsDuplicateHeader > -1) {
      const accumulatesValue = {};

      Object.keys(item).forEach((key) => {
        if (key.search("duplicate") > -1) {
          const [newKey] = key.split("_");

          if (typeof item[key] === "string") {
            const values = item[key].split(/[/,/]/);

            const removeDuplicateValues = [
              ...(accumulatesValue[newKey] || []),
              ...values.map((value) => value.trim()),
            ];

            accumulatesValue[newKey] = [...new Set(removeDuplicateValues)];
          }
          Reflect.deleteProperty(item, key);
        }
      });
      Object.entries(accumulatesValue).forEach(([key, value]) => {
        Reflect.set(item, key, value);
      });
    }

    const existsDuplicateItem = newData.findIndex(
      (data) => data.eid === item.eid
    );

    if (existsDuplicateItem > -1) {
      Object.entries(newData[existsDuplicateItem]).forEach(([key, value]) => {
        if (Array.isArray(value)) {
          const newValue = [...item[key], ...value];
          newData[existsDuplicateItem][key] = [...new Set(newValue)];
        }
      });
    } else {
      newData.push(item);
    }
  });

  return newData;
};

formatData();
