export function overrideSecrets(fs: any): void {
    const secretPath = process.env.SECRET_PATH;
    if (!secretPath) {
        return;
    }
    const secrets = JSON.parse(fs.readFileSync(secretPath, "utf8"));
    for (const key in secrets) {
        process.env[key] = secrets[key];
    }
}
