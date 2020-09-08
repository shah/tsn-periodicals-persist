import * as qc from "@shah/queryable-content";
import * as pipe from "@shah/ts-pipe";
import * as atc from "@shah/tsn-content-classification-html-anchors";
import * as atcRulesCommon from "@shah/tsn-content-classification-html-anchors/dist/html-anchor-text-classification-rules-common";
import * as pd from "@shah/tsn-periodicals";
import { Expect, SetupFixture, Test, TestCase, TestFixture } from "alsatian";
import * as fs from "fs";
import * as path from "path";
import mime from "whatwg-mimetype";
import * as pp from "./periodicals-persist";

export interface EmailSupplierContent {
    messageId: string;
    fromAddress: string;
    fromName: string;
    date: string;
    subject: string;
    htmlContent: string
}

export interface EmailPeriodicalEdition extends pd.PeriodicalEdition {
    readonly subject: string;
}

export class EmailPeriodicalEditionPropsSupplier implements pp.PersistPropsTransformer {
    static readonly singleton = new EmailPeriodicalEditionPropsSupplier();

    flow(ctx: pp.PersistPropsTransformContext<EmailPeriodicalEdition>, suggested: pp.PersistProperties): pp.PersistProperties {
        return { ...suggested, subject: ctx.source.subject };
    }
}

@TestFixture("Periodicals Persistence")
export class EmailTestSuite {
    readonly destPath = "email-supplier-test-results";
    readonly contentTr: qc.ContentTransformer = pipe.pipe(qc.EnrichQueryableHtmlContent.singleton);
    readonly testEmails: EmailSupplierContent[] = require("./email-supplier-test-content.json");
    readonly atcRulesEngine = new atc.TypicalAnchorTextRuleEngine(atcRulesCommon.commonRules);
    readonly atcClassifier = atc.TypicalAnchorTextClassifier.singleton;
    readonly supplier = new pd.TypicalPeriodicalSupplier("email://test", this.atcRulesEngine, this.atcClassifier);
    readonly stats = {
        editionsEncountered: 0,
        periodicalsEncountered: 0,
        editionAnchorsEncountered: 0,
    }

    constructor() {
    }

    @SetupFixture
    public async classifyEmailNewsletters(): Promise<void> {
        const periodicalsEncountered: { [name: string]: pd.Periodical } = {};
        for (const email of this.testEmails) {
            let periodical = this.supplier.registerPeriodical(`${email.fromName} <${email.fromAddress}>`);
            if (!periodicalsEncountered[periodical.name]) {
                periodicalsEncountered[periodical.name] = periodical;
                this.stats.periodicalsEncountered++;
            }
            const date = new Date(email.date);
            const content = await this.contentTr.flow({
                htmlSource: email.htmlContent,
                uri: `email://${email.messageId}/${email.fromAddress}/${email.fromName}/${date.toISOString()}/${email.subject}`
            }, {
                contentType: "text/html",
                mimeType: new mime("text/html"),
            }) as qc.QueryableHtmlContent;
            const anchors: pd.ClassifiedAnchor[] = [];
            content.anchors().map((anchor) => {
                anchors.push(periodical.registerAnchor(anchor))
            });
            this.stats.editionAnchorsEncountered += anchors.length;
            const pe: EmailPeriodicalEdition = {
                supplierContentId: email.messageId,
                fromAddress: email.fromAddress,
                fromName: email.fromName,
                date: date,
                anchors: anchors,
                subject: email.subject,
            }
            periodical.registerEdition(pe);
            this.stats.editionsEncountered++;
        }
        await this.supplier.classifyAnchors();
        this.stats.periodicalsEncountered = Object.keys(periodicalsEncountered).length;
    }

    @SetupFixture
    public persistPeriodicals(): void {
        pp.DefaultRelationalCsvTableWriters.recreateDir(this.destPath);
        const writerNames = pp.DefaultRelationalCsvTableWriters.NAMES;
        const writers = new pp.DefaultRelationalCsvTableWriters(this.destPath, pp.DefaultRelationalCsvTableWriters.UUID_NAMESPACE, {
            names: writerNames,
            periodicalEditions: new pp.TabularWriter({
                destPath: this.destPath, fileName: writerNames.periodicalEditions,
                parentUuidNamespace: pp.DefaultRelationalCsvTableWriters.UUID_NAMESPACE,
                ppTransform: EmailPeriodicalEditionPropsSupplier.singleton
            })
        });
        const db = new pp.PersistRelationalCSV(writers);
        db.persistSupplier(this.supplier);
        writers.close();
    }

    @TestCase("suppliers.csv")
    @TestCase("periodicals.csv")
    @TestCase("periodical-anchors.csv")
    @TestCase("periodical-anchors-common.csv")
    @TestCase("periodical-editions.csv")
    @TestCase("periodical-edition-anchors.csv")
    @Test("Ensure files created")
    public testOutputFileCreated(fileName: string): void {
        Expect(fs.existsSync(path.join(this.destPath, fileName))).toBe(true);
    }
}
