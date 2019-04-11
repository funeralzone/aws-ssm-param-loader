const AWS = require('aws-sdk');
const ssm = new AWS.SSM();

const load = async (keys, ssmPrefix) => {

    const validateParams = (keys, params) => {
        const missing = keys.filter(k => params[k] === undefined);
        if (missing.length > 0) {
            throw new Error(`missing keys: ${missing}`);
        }
    };

    let result = {};
    const maxChunkSize = 10;

    for (let i = 0; i < keys.length; i += maxChunkSize) {
        let chunk = keys.slice(i, i + maxChunkSize);

        let requestParameters = {
            Names: chunk.map(k => `${ssmPrefix}${k}`),
            WithDecryption: true
        };

        let responseParameters = await ssm.getParameters(requestParameters).promise();

        for (let parameter of responseParameters.Parameters) {
            let name = parameter.Name.replace(ssmPrefix, '');
            result[name] = parameter.Value;
        }
    }

    validateParams(keys, result);

    return result;
};

export default load;