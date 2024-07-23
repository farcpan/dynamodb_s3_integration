import { App } from "aws-cdk-lib";
import { SrcStack } from "../lib/src-stack";

const app = new App();
new SrcStack(app, "SrcStack", {});
