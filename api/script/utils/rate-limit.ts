export function keyGenerator(req): string {
  const xForwardedForHeader = req?.headers['x-forwarded-for'];

  let forwarded = '';
  if (xForwardedForHeader?.constructor?.name == "Array") {
    forwarded = req.headers['x-forwarded-for'][0] as string;
  } else {
    forwarded = req.headers['x-forwarded-for'] as string;
  }
  return forwarded ? forwarded.split(',')[0] : req.ip; // 첫 번째 IP 추출
}
